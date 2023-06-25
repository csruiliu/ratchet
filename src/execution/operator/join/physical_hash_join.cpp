#include "duckdb/execution/operator/join/physical_hash_join.hpp"

#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include <iostream>

#include "json.hpp"
using json = nlohmann::json;
#include <fstream>
#include <dirent.h>
#include <regex>


namespace duckdb {

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   const vector<idx_t> &left_projection_map,
                                   const vector<idx_t> &right_projection_map_p, vector<LogicalType> delim_types,
                                   idx_t estimated_cardinality, PerfectHashJoinStats perfect_join_stats)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, std::move(cond), join_type, estimated_cardinality),
      right_projection_map(right_projection_map_p), delim_types(std::move(delim_types)),
      perfect_join_statistics(std::move(perfect_join_stats)) {

	children.push_back(std::move(left));
	children.push_back(std::move(right));

	D_ASSERT(left_projection_map.empty());
	for (auto &condition : conditions) {
		condition_types.push_back(condition.left->return_type);
	}

	// for ANTI, SEMI and MARK join, we only need to store the keys, so for these the build types are empty
	if (join_type != JoinType::ANTI && join_type != JoinType::SEMI && join_type != JoinType::MARK) {
		build_types = LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map);
	}
}

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   idx_t estimated_cardinality, PerfectHashJoinStats perfect_join_state)
    : PhysicalHashJoin(op, std::move(left), std::move(right), std::move(cond), join_type, {}, {}, {},
                       estimated_cardinality, std::move(perfect_join_state)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashJoinGlobalSinkState : public GlobalSinkState {
public:
	HashJoinGlobalSinkState(const PhysicalHashJoin &op, ClientContext &context)
	    : finalized(false), scanned_data(false) {
#if RATCHET_PRINT == 1
        std::cout << "[HashJoinGlobalSinkState] Construction" << std::endl;
#endif
        hash_table = op.InitializeHashTable(context);

		// for perfect hash join
		perfect_join_executor = make_unique<PerfectHashJoinExecutor>(op, *hash_table, op.perfect_join_statistics);
		// for external hash join
		external = op.can_go_external && ClientConfig::GetConfig(context).force_external;
		// memory usage per thread scales with max mem / num threads
		double max_memory = BufferManager::GetBufferManager(context).GetMaxMemory();
		double num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
		// HT may not exceed 60% of memory
		max_ht_size = max_memory * 0.6;
		sink_memory_per_thread = max_ht_size / num_threads;
		// Set probe types
		const auto &payload_types = op.children[0]->types;
		probe_types.insert(probe_types.end(), op.condition_types.begin(), op.condition_types.end());
		probe_types.insert(probe_types.end(), payload_types.begin(), payload_types.end());
		probe_types.emplace_back(LogicalType::HASH);
	}

	void ScheduleFinalize(Pipeline &pipeline, Event &event);
	void InitializeProbeSpill(ClientContext &context);

public:
	//! Global HT used by the join
	unique_ptr<JoinHashTable> hash_table;
	//! The perfect hash join executor (if any)
	unique_ptr<PerfectHashJoinExecutor> perfect_join_executor;
	//! Whether or not the hash table has been finalized
	bool finalized = false;

	//! Whether we are doing an external join
	bool external;
	//! Memory usage per thread during the Sink and Execute phases
	idx_t max_ht_size;
	idx_t sink_memory_per_thread;

	//! Hash tables built by each thread
	mutex lock;
	vector<unique_ptr<JoinHashTable>> local_hash_tables;

	//! Excess probe data gathered during Sink
	vector<LogicalType> probe_types;
	unique_ptr<JoinHashTable::ProbeSpill> probe_spill;

	//! Whether or not we have started scanning data using GetData
	atomic<bool> scanned_data;
};

class HashJoinLocalSinkState : public LocalSinkState {
public:
	HashJoinLocalSinkState(const PhysicalHashJoin &op, ClientContext &context) : build_executor(context) {
		auto &allocator = Allocator::Get(context);
		if (!op.right_projection_map.empty()) {
			build_chunk.Initialize(allocator, op.build_types);
		}
		for (auto &cond : op.conditions) {
			build_executor.AddExpression(*cond.right);
		}
		join_keys.Initialize(allocator, op.condition_types);

		hash_table = op.InitializeHashTable(context);
	}

public:
	DataChunk build_chunk;
	DataChunk join_keys;
	ExpressionExecutor build_executor;

	//! Thread-local HT
	unique_ptr<JoinHashTable> hash_table;
};

unique_ptr<JoinHashTable> PhysicalHashJoin::InitializeHashTable(ClientContext &context) const {
	auto result =
	    make_unique<JoinHashTable>(BufferManager::GetBufferManager(context), conditions, build_types, join_type);
	if (!delim_types.empty() && join_type == JoinType::MARK) {
		// correlated MARK join
		if (delim_types.size() + 1 == conditions.size()) {
			// the correlated MARK join has one more condition than the amount of correlated columns
			// this is the case in a correlated ANY() expression
			// in this case we need to keep track of additional entries, namely:
			// - (1) the total amount of elements per group
			// - (2) the amount of non-null elements per group
			// we need these to correctly deal with the cases of either:
			// - (1) the group being empty [in which case the result is always false, even if the comparison is NULL]
			// - (2) the group containing a NULL value [in which case FALSE becomes NULL]
			auto &info = result->correlated_mark_join_info;

			vector<LogicalType> payload_types;
			vector<BoundAggregateExpression *> correlated_aggregates;
			unique_ptr<BoundAggregateExpression> aggr;

			// jury-rigging the GroupedAggregateHashTable
			// we need a count_star and a count to get counts with and without NULLs

			FunctionBinder function_binder(context);
			aggr = function_binder.BindAggregateFunction(CountStarFun::GetFunction(), {}, nullptr,
			                                             AggregateType::NON_DISTINCT);
			correlated_aggregates.push_back(&*aggr);
			payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(std::move(aggr));

			auto count_fun = CountFun::GetFunction();
			vector<unique_ptr<Expression>> children;
			// this is a dummy but we need it to make the hash table understand whats going on
			children.push_back(make_unique_base<Expression, BoundReferenceExpression>(count_fun.return_type, 0));
			aggr = function_binder.BindAggregateFunction(count_fun, std::move(children), nullptr,
			                                             AggregateType::NON_DISTINCT);
			correlated_aggregates.push_back(&*aggr);
			payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(std::move(aggr));

			auto &allocator = Allocator::Get(context);
			info.correlated_counts = make_unique<GroupedAggregateHashTable>(context, allocator, delim_types,
			                                                                payload_types, correlated_aggregates);
			info.correlated_types = delim_types;
			info.group_chunk.Initialize(allocator, delim_types);
			info.result_chunk.Initialize(allocator, payload_types);
		}
	}
	return result;
}

unique_ptr<GlobalSinkState> PhysicalHashJoin::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<HashJoinGlobalSinkState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashJoin::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<HashJoinLocalSinkState>(*this, context.client);
}

SinkResultType PhysicalHashJoin::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                      DataChunk &input) const {
#if RATCHET_PRINT == 1
    std::cout << "[PhysicalHashJoin::Sink] for pipeline " << context.pipeline->GetPipelineId() << std::endl;
#endif
    auto &gstate = (HashJoinGlobalSinkState &)gstate_p;
	auto &lstate = (HashJoinLocalSinkState &)lstate_p;

	// resolve the join keys for the right chunk
	lstate.join_keys.Reset();
	lstate.build_executor.Execute(input, lstate.join_keys);

	// build the HT
	auto &ht = *lstate.hash_table;
	if (!right_projection_map.empty()) {
		// there is a projection map: fill the build chunk with the projected columns
		lstate.build_chunk.Reset();
		lstate.build_chunk.SetCardinality(input);
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			lstate.build_chunk.data[i].Reference(input.data[right_projection_map[i]]);
		}
		ht.Build(lstate.join_keys, lstate.build_chunk);
	} else if (!build_types.empty()) {
		// there is not a projected map: place the entire right chunk in the HT
		ht.Build(lstate.join_keys, input);
	} else {
		// there are only keys: place an empty chunk in the payload
		lstate.build_chunk.SetCardinality(input.size());
		ht.Build(lstate.join_keys, lstate.build_chunk);
	}

    // Serialization for external hash join
    if (global_suspend && gstate.external) {
        std::cout << "== Serialization for external hash join ==" << std::endl;
        D_ASSERT(lstate.join_keys.size() == lstate.build_chunk.size());

        std::chrono::steady_clock::time_point suspend_check = std::chrono::steady_clock::now();
        uint64_t time_dur_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                suspend_check - global_start).count();

        if (time_dur_ms > global_suspend_point_ms) {
            global_suspend_start = true;
            json json_data;

            global_finalized_pipelines.emplace_back(context.pipeline->GetPipelineId());
            idx_t build_size = lstate.join_keys.size();

            for (idx_t i = 0; i < lstate.join_keys.GetTypes().size(); i++) {
                if (lstate.join_keys.GetTypes()[i] == LogicalType::INTEGER) {
                    vector<int64_t> key_vector;
                    for (idx_t j = 0; j < build_size; j++) {
                        key_vector.emplace_back(std::stoi(lstate.join_keys.data[i].GetValue(j).ToString()));
                    }
                    json_data["join_key_" + to_string(i)]["type"] = LogicalType::INTEGER;
                    json_data["join_key_" + to_string(i)]["data"] = key_vector;
                } else if (lstate.join_keys.GetTypes()[i] == LogicalType::VARCHAR) {
                    vector<string> key_vector;
                    for (idx_t j = 0; j < build_size; j++) {
                        key_vector.emplace_back(lstate.join_keys.data[i].GetValue(j).ToString());
                    }
                    json_data["join_key_" + to_string(i)]["type"] = LogicalType::VARCHAR;
                    json_data["join_key_" + to_string(i)]["data"] = key_vector;
                } else {
                    throw ParserException("Cannot recognize build types");
                }
            }

            for (idx_t i = 0; i < lstate.build_chunk.GetTypes().size(); i++) {
                if (lstate.build_chunk.GetTypes()[i] == LogicalType::INTEGER) {
                    vector<int64_t> value_vector;
                    for (idx_t j = 0; j < build_size; j++) {
                        value_vector.emplace_back(stoi(lstate.build_chunk.data[i].GetValue(j).ToString()));
                    }
                    json_data["build_chunk_" + to_string(i)]["type"] = LogicalType::INTEGER;
                    json_data["build_chunk_" + to_string(i)]["data"] = value_vector;
                } else if (lstate.build_chunk.GetTypes()[i] == LogicalType::VARCHAR) {
                    vector<string> value_vector;
                    for (idx_t j = 0; j < build_size; j++) {
                        value_vector.emplace_back(lstate.build_chunk.data[i].GetValue(j).ToString());
                    }
                    json_data["build_chunk_" + to_string(i)]["type"] = LogicalType::VARCHAR;
                    json_data["build_chunk_" + to_string(i)]["data"] = value_vector;
                } else {
                    throw ParserException("Cannot recognize key types");
                };
            }

            json_data["pipeline_ids"] = global_finalized_pipelines;
            json_data["build_size"] = build_size;

            string suspend_folder = global_suspend_folder;

#if RATCHET_SERDE_FORMAT == 0
            std::ofstream outputFile(suspend_folder.append("/part-").append(to_string(global_ht_partition)).append(".ratchet"),
                                     std::ios::out | std::ios::binary);
            const auto output_vector = json::to_cbor(json_data);
            outputFile.write(reinterpret_cast<const char *>(output_vector.data()), output_vector.size());
#elif RATCHET_SERDE_FORMAT == 1
            std::ofstream outputFile(suspend_folder.append("/part-").append(to_string(global_ht_partition)).append(".ratchet"));
            outputFile << json_data;
#endif
            outputFile.close();
            if (outputFile.fail()) {
                std::cerr << "Error writing to file!" << std::endl;
            }

            global_ht_partition++;
        }
    }

	// swizzle if we reach memory limit
	auto approx_ptr_table_size = ht.Count() * 3 * sizeof(data_ptr_t);
	if (can_go_external && ht.SizeInBytes() + approx_ptr_table_size >= gstate.sink_memory_per_thread) {
		lstate.hash_table->SwizzleBlocks();
		gstate.external = true;
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalHashJoin::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
#if RATCHET_PRINT == 1
    std::cout << "[PhysicalHashJoin::Combine] for pipeline " << context.pipeline->GetPipelineId() << std::endl;
#endif
    auto &gstate = (HashJoinGlobalSinkState &)gstate_p;
	auto &lstate = (HashJoinLocalSinkState &)lstate_p;
	if (lstate.hash_table) {
		lock_guard<mutex> local_ht_lock(gstate.lock);
		gstate.local_hash_tables.push_back(std::move(lstate.hash_table));
	}
	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(this, &lstate.build_executor, "build_executor", 1);
	client_profiler.Flush(context.thread.profiler);
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class HashJoinFinalizeTask : public ExecutorTask {
public:
	HashJoinFinalizeTask(shared_ptr<Event> event_p, ClientContext &context, HashJoinGlobalSinkState &sink,
	                     idx_t block_idx_start, idx_t block_idx_end, bool parallel)
	    : ExecutorTask(context), event(std::move(event_p)), sink(sink), block_idx_start(block_idx_start),
	      block_idx_end(block_idx_end), parallel(parallel) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
#if RATCHET_PRINT == 1
        std::cout << "[HashJoinFinalizeTask] ExecuteTask start " << block_idx_start << "," << block_idx_end << std::endl;
#endif
        sink.hash_table->Finalize(block_idx_start, block_idx_end, parallel);
        event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	HashJoinGlobalSinkState &sink;
	idx_t block_idx_start;
	idx_t block_idx_end;
	bool parallel;
};

class HashJoinFinalizeEvent : public BasePipelineEvent {
public:
	HashJoinFinalizeEvent(Pipeline &pipeline_p, HashJoinGlobalSinkState &sink)
	    : BasePipelineEvent(pipeline_p), sink(sink) {
	}

	HashJoinGlobalSinkState &sink;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();

		vector<unique_ptr<Task>> finalize_tasks;
		auto &ht = *sink.hash_table;
		const auto &block_collection = ht.GetBlockCollection();
		const auto &blocks = block_collection.blocks;
		const auto num_blocks = blocks.size();
		if (block_collection.count < PARALLEL_CONSTRUCT_THRESHOLD && !context.config.verify_parallelism) {
			// Single-threaded finalize
			finalize_tasks.push_back(
			    make_unique<HashJoinFinalizeTask>(shared_from_this(), context, sink, 0, num_blocks, false));
		} else {
			// Parallel finalize
			idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
			auto blocks_per_thread = MaxValue<idx_t>((num_blocks + num_threads - 1) / num_threads, 1);

			idx_t block_idx = 0;
			for (idx_t thread_idx = 0; thread_idx < num_threads; thread_idx++) {
				auto block_idx_start = block_idx;
				auto block_idx_end = MinValue<idx_t>(block_idx_start + blocks_per_thread, num_blocks);
				finalize_tasks.push_back(make_unique<HashJoinFinalizeTask>(shared_from_this(), context, sink,
				                                                           block_idx_start, block_idx_end, true));
				block_idx = block_idx_end;
				if (block_idx == num_blocks) {
					break;
				}
			}
		}
		SetTasks(std::move(finalize_tasks));
	}

	void FinishEvent() override {
		sink.hash_table->finalized = true;
	}

	static constexpr const idx_t PARALLEL_CONSTRUCT_THRESHOLD = 1048576;
};

void HashJoinGlobalSinkState::ScheduleFinalize(Pipeline &pipeline, Event &event) {
	if (hash_table->Count() == 0) {
		hash_table->finalized = true;
		return;
	}
	hash_table->InitializePointerTable();
	auto new_event = make_shared<HashJoinFinalizeEvent>(pipeline, *this);
	event.InsertEvent(std::move(new_event));
}

void HashJoinGlobalSinkState::InitializeProbeSpill(ClientContext &context) {
	lock_guard<mutex> guard(lock);
	if (!probe_spill) {
		probe_spill = make_unique<JoinHashTable::ProbeSpill>(*hash_table, context, probe_types);
	}
}

class HashJoinPartitionTask : public ExecutorTask {
public:
	HashJoinPartitionTask(shared_ptr<Event> event_p, ClientContext &context, JoinHashTable &global_ht,
	                      JoinHashTable &local_ht)
	    : ExecutorTask(context), event(std::move(event_p)), global_ht(global_ht), local_ht(local_ht) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
#if RATCHET_PRINT == 1
        std::cout << "[HashJoinPartitionTask] ExecuteTask" << std::endl;
#endif
        local_ht.Partition(global_ht);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;

	JoinHashTable &global_ht;
	JoinHashTable &local_ht;
};

class HashJoinPartitionEvent : public BasePipelineEvent {
public:
	HashJoinPartitionEvent(Pipeline &pipeline_p, HashJoinGlobalSinkState &sink,
	                       vector<unique_ptr<JoinHashTable>> &local_hts)
	    : BasePipelineEvent(pipeline_p), sink(sink), local_hts(local_hts) {
	}

	HashJoinGlobalSinkState &sink;
	vector<unique_ptr<JoinHashTable>> &local_hts;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();
		vector<unique_ptr<Task>> partition_tasks;
		partition_tasks.reserve(local_hts.size());
		for (auto &local_ht : local_hts) {
			partition_tasks.push_back(
			    make_unique<HashJoinPartitionTask>(shared_from_this(), context, *sink.hash_table, *local_ht));
		}
		SetTasks(std::move(partition_tasks));
	}

	void FinishEvent() override {
		local_hts.clear();
		sink.hash_table->PrepareExternalFinalize();
		sink.ScheduleFinalize(*pipeline, *this);
	}
};

SinkFinalizeType PhysicalHashJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            GlobalSinkState &gstate) const {
#if RATCHET_PRINT == 1
    std::cout << "[PhysicalHashJoin::Finalize] for pipeline " << pipeline.GetPipelineId() << std::endl;
#endif
    auto &sink = (HashJoinGlobalSinkState &)gstate;

    auto use_perfect_hash = sink.perfect_join_executor->CanDoPerfectHashJoin();

    idx_t current_id = pipeline.GetPipelineId();
    auto it = std::find(global_finalized_pipelines.begin(),global_finalized_pipelines.end(),current_id);

    //! Resume Process for Finalize
    if (global_resume && it != global_finalized_pipelines.end()) {
        if (!sink.external) {
            std::cout << "== Resume Perfect Hash Join ==" << std::endl;
            sink.hash_table->Reset();
#if RATCHET_SERDE_FORMAT == 0
            std::ifstream input_file(global_resume_file, std::ios::binary);
            std::vector<uint8_t> input_vector((std::istreambuf_iterator<char>(input_file)),std::istreambuf_iterator<char>());
            json json_data = json::from_cbor(input_vector);
#elif RATCHET_SERDE_FORMAT == 1
            std::ifstream f(global_resume_file);
            json json_data = json::parse(f);
#endif
            // idx_t build_size = build_vector_str.size();
            auto build_size = (idx_t)json_data["build_size"];

            unique_ptr<JoinHashTable> hash_table;
            hash_table = this->InitializeHashTable(context);

            for (idx_t i = 0; i < build_types.size(); i++) {
                //! For building values
                LogicalType build_type = LogicalType((LogicalTypeId)json_data["build_chunk_" + to_string(i)]["type"]);
                Vector build_chunk_vector = Vector(build_type, true, false, build_size);
                DataChunk build_chunk;
                build_chunk.SetCardinality(build_size);

                if (build_type == LogicalType::VARCHAR) {
                    vector<string> build_vector_data = json_data["build_chunk_" + to_string(i)]["data"];
                    for (idx_t j = 0; j < build_size; j++) {
                        build_chunk_vector.SetValue(j, Value(build_vector_data[j]));
                    }
                    build_chunk.data.emplace_back(build_chunk_vector);
                } else if (build_type == LogicalType::INTEGER) {
                    vector<int64_t> build_vector_data = json_data["build_chunk_" + to_string(i)]["data"];
                    for (idx_t j = 0; j < build_size; j++) {
                        build_chunk_vector.SetValue(j, Value(build_vector_data[j]));
                    }
                    build_chunk.data.emplace_back(build_chunk_vector);
                } else {
                    throw ParserException("Cannot recognize build types");
                }

                //! For building keys
                LogicalType key_type = LogicalType((LogicalTypeId)json_data["join_key_" + to_string(i)]["type"]);
                Vector join_keys_vector = Vector(key_type, true, false, build_size);
                DataChunk join_keys;
                join_keys.SetCardinality(build_size);

                if (key_type == LogicalType::VARCHAR) {
                    vector<string> join_key_data = json_data["join_key_" + to_string(i)]["data"];
                    for (idx_t j = 0; j < build_size; j++) {
                        join_keys_vector.SetValue(j, Value(join_key_data[j]));
                    }
                    join_keys.data.emplace_back(join_keys_vector);
                } else if (key_type == LogicalType::INTEGER) {
                    vector<int64_t> join_key_data = json_data["join_key_" + to_string(i)]["data"];
                    for (idx_t j = 0; j < build_size; j++) {
                        join_keys_vector.SetValue(j, Value(join_key_data[j]));
                    }
                    join_keys.data.emplace_back(join_keys_vector);
                } else {
                    throw ParserException("Cannot recognize key types");
                }

                // build hash table using join_keys and build_chunk
                hash_table->Build(join_keys, build_chunk);
                sink.hash_table->Merge(*hash_table);
            }
            // auto use_perfect_hash = sink.perfect_join_executor->CanDoPerfectHashJoin();
            if (use_perfect_hash) {
                // D_ASSERT(sink.hash_table->equality_types.size() == 1);
                auto key_type = sink.hash_table->equality_types[0];
                use_perfect_hash = sink.perfect_join_executor->BuildPerfectHashTable(key_type);
            }
            if (!use_perfect_hash) {
                sink.perfect_join_executor.reset();
                sink.ScheduleFinalize(pipeline, event);
            }
            sink.finalized = true;
            return SinkFinalizeType::READY;
        } else {
            D_ASSERT(can_go_external);
            std::cout << "== Resume External Hash Join ==" << std::endl;
            sink.hash_table->Reset();

            DIR *dir;
            struct dirent *ent;

            std::regex fileNameRegex("^part-.*\\.ratchet$");
            if ((dir = opendir(global_resume_folder.c_str())) != nullptr) {
                while ((ent = readdir(dir)) != nullptr) {
                    std::string fileName = ent->d_name;

                    if (std::regex_match(fileName, fileNameRegex)) {
                        string resume_folder = global_resume_folder;

#if RATCHET_SERDE_FORMAT == 0
                        std::ifstream input_file(resume_folder.append("/").append(fileName), std::ios::binary);
                        std::vector<uint8_t> input_vector((std::istreambuf_iterator<char>(input_file)),std::istreambuf_iterator<char>());
                        json json_data = json::from_cbor(input_vector);
#elif RATCHET_SERDE_FORMAT == 1
                        std::ifstream f(resume_folder.append("/").append(fileName));
                        json json_data = json::parse(f);
#endif
                        // idx_t build_size = build_vector_str.size();
                        auto build_size = (idx_t)json_data["build_size"];

                        unique_ptr<JoinHashTable> hash_table;
                        hash_table = this->InitializeHashTable(context);
                        for (idx_t i = 0; i < build_types.size(); i++) {
                            //! For building values
                            LogicalType build_type = LogicalType((LogicalTypeId)json_data["build_chunk_" + to_string(i)]["type"]);
                            Vector build_chunk_vector = Vector(build_type, true, false, build_size);
                            DataChunk build_chunk;
                            build_chunk.SetCardinality(build_size);

                            if (build_type == LogicalType::VARCHAR) {
                                vector<string> build_vector_data = json_data["build_chunk_" + to_string(i)]["data"];
                                for (idx_t j = 0; j < build_size; j++) {
                                    build_chunk_vector.SetValue(j, Value(build_vector_data[j]));
                                }
                                build_chunk.data.emplace_back(build_chunk_vector);
                            } else if (build_type == LogicalType::INTEGER) {
                                vector<int64_t> build_vector_data = json_data["build_chunk_" + to_string(i)]["data"];
                                for (idx_t j = 0; j < build_size; j++) {
                                    build_chunk_vector.SetValue(j, Value(build_vector_data[j]));
                                }
                                build_chunk.data.emplace_back(build_chunk_vector);
                            } else {
                                throw ParserException("Cannot recognize build types");
                            }

                            //! For building keys
                            LogicalType key_type = LogicalType((LogicalTypeId)json_data["join_key_" + to_string(i)]["type"]);
                            Vector join_keys_vector = Vector(key_type, true, false, build_size);
                            DataChunk join_keys;
                            join_keys.SetCardinality(build_size);

                            if (key_type == LogicalType::VARCHAR) {
                                vector<string> join_key_data = json_data["join_key_" + to_string(i)]["data"];
                                for (idx_t j = 0; j < build_size; j++) {
                                    join_keys_vector.SetValue(j, Value(join_key_data[j]));
                                }
                                join_keys.data.emplace_back(join_keys_vector);
                            } else if (key_type == LogicalType::INTEGER) {
                                vector<int64_t> join_key_data = json_data["join_key_" + to_string(i)]["data"];
                                for (idx_t j = 0; j < build_size; j++) {
                                    join_keys_vector.SetValue(j, Value(join_key_data[j]));
                                }
                                join_keys.data.emplace_back(join_keys_vector);
                            } else {
                                throw ParserException("Cannot recognize key types");
                            }

                            // build hash table using join_keys and build_chunk
                            hash_table->Build(join_keys, build_chunk);
                            // sink.hash_table->Merge(*hash_table);
                        }
                        sink.local_hash_tables.push_back(std::move(hash_table));
                    }
                }
                closedir(dir);
            } else {
                std::cerr << "Failed to open the folder." << std::endl;
            }

            // External join - partition HT
            sink.perfect_join_executor.reset();

            sink.hash_table->ComputePartitionSizes(context.config, sink.local_hash_tables, sink.max_ht_size);
            auto new_event = make_shared<HashJoinPartitionEvent>(pipeline, sink, sink.local_hash_tables);
            event.InsertEvent(std::move(new_event));
            sink.finalized = true;
            return SinkFinalizeType::READY;
        }
    }

    //! Suspend Process for Finalize
    // if external hash join, checking suspend and serializing states happened in Sink()
    if (sink.external && global_suspend_start) {
        exit(0);
    }
    // if not external hash join, checking suspend and serializing states happened here
    if (!sink.external && global_suspend) {
        std::chrono::steady_clock::time_point suspend_check = std::chrono::steady_clock::now();
        uint64_t time_dur_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                suspend_check - global_start).count();
        if (time_dur_ms > global_suspend_point_ms) {
            global_suspend_start = true;
            for (auto &local_ht : sink.local_hash_tables) {
                sink.hash_table->Merge(*local_ht);
            }
            sink.local_hash_tables.clear();

            if (use_perfect_hash) {
                D_ASSERT(sink.hash_table->equality_types.size() == 1);
                auto key_type = sink.hash_table->equality_types[0];
                use_perfect_hash = sink.perfect_join_executor->BuildPerfectHashTable(key_type);
            }

            global_finalized_pipelines.emplace_back(pipeline.GetPipelineId());
            // Serialize PerfectHashTable to Disk
            sink.perfect_join_executor->SerializePerfectHashTable();
            exit(0);
        }
    }

    //! Regular Process for Finalize
    if (sink.external) {
        D_ASSERT(can_go_external);
        // External join - partition HT
        sink.perfect_join_executor.reset();
        sink.hash_table->ComputePartitionSizes(context.config, sink.local_hash_tables, sink.max_ht_size);
        auto new_event = make_shared<HashJoinPartitionEvent>(pipeline, sink, sink.local_hash_tables);
        event.InsertEvent(std::move(new_event));
        sink.finalized = true;
        return SinkFinalizeType::READY;
    } else {
        for (auto &local_ht : sink.local_hash_tables) {
            sink.hash_table->Merge(*local_ht);
        }
        sink.local_hash_tables.clear();
    }

    // check for possible perfect hash table
    // auto use_perfect_hash = sink.perfect_join_executor->CanDoPerfectHashJoin();
    if (use_perfect_hash) {
        D_ASSERT(sink.hash_table->equality_types.size() == 1);
        auto key_type = sink.hash_table->equality_types[0];
        use_perfect_hash = sink.perfect_join_executor->BuildPerfectHashTable(key_type);
    }

    // In case of a large build side or duplicates, use regular hash join
    if (!use_perfect_hash) {
        sink.perfect_join_executor.reset();
        sink.ScheduleFinalize(pipeline, event);
    }
    sink.finalized = true;
    if (sink.hash_table->Count() == 0 && EmptyResultIfRHSIsEmpty()) {
        return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
    }
    return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class HashJoinOperatorState : public CachingOperatorState {
public:
	explicit HashJoinOperatorState(ClientContext &context) : probe_executor(context), initialized(false) {
	}

	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
	unique_ptr<OperatorState> perfect_hash_join_state;

	bool initialized;
	JoinHashTable::ProbeSpillLocalAppendState spill_state;
	//! Chunk to sink data into for external join
	DataChunk spill_chunk;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, &probe_executor, "probe_executor", 0);
	}
};

unique_ptr<OperatorState> PhysicalHashJoin::GetOperatorState(ExecutionContext &context) const {
	auto &allocator = Allocator::Get(context.client);
	auto &sink = (HashJoinGlobalSinkState &)*sink_state;
	auto state = make_unique<HashJoinOperatorState>(context.client);
	if (sink.perfect_join_executor) {
		state->perfect_hash_join_state = sink.perfect_join_executor->GetOperatorState(context);
	} else {
		state->join_keys.Initialize(allocator, condition_types);
		for (auto &cond : conditions) {
			state->probe_executor.AddExpression(*cond.left);
		}
	}
	if (sink.external) {
		state->spill_chunk.Initialize(allocator, sink.probe_types);
		sink.InitializeProbeSpill(context.client);
	}

	return std::move(state);
}

OperatorResultType PhysicalHashJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     GlobalOperatorState &gstate, OperatorState &state_p) const {
#if RATCHET_PRINT == 1
    std::cout << "[PhysicalHashJoin::ExecuteInternal] for pipeline " << context.pipeline->GetPipelineId() << std::endl;
#endif
    auto &state = (HashJoinOperatorState &)state_p;
	auto &sink = (HashJoinGlobalSinkState &)*sink_state;
	D_ASSERT(sink.finalized);
	D_ASSERT(!sink.scanned_data);

	// some initialization for external hash join
	if (sink.external && !state.initialized) {
		if (!sink.probe_spill) {
			sink.InitializeProbeSpill(context.client);
		}
		state.spill_state = sink.probe_spill->RegisterThread();
		state.initialized = true;
	}

    if (sink.hash_table->Count() == 0 && EmptyResultIfRHSIsEmpty()) {
        return OperatorResultType::FINISHED;
    }

	if (sink.perfect_join_executor) {
		D_ASSERT(!sink.external);
		return sink.perfect_join_executor->ProbePerfectHashTable(context, input, chunk, *state.perfect_hash_join_state);
	}

	if (state.scan_structure) {
		// still have elements remaining (i.e. we got >STANDARD_VECTOR_SIZE elements in the previous probe)
		state.scan_structure->Next(state.join_keys, input, chunk);
		if (chunk.size() > 0) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
		state.scan_structure = nullptr;
		return OperatorResultType::NEED_MORE_INPUT;
	}

    // probe the HT
    if (sink.hash_table->Count() == 0) {
        ConstructEmptyJoinResult(sink.hash_table->join_type, sink.hash_table->has_null, input, chunk);
        return OperatorResultType::NEED_MORE_INPUT;
    }

	// resolve the join keys for the left chunk
	state.join_keys.Reset();
	state.probe_executor.Execute(input, state.join_keys);

    // state.join_keys.Print();
    // input.Print();

	// perform the actual probe
	if (sink.external) {
        // split the original input to input + state.spill_chunk
		state.scan_structure = sink.hash_table->ProbeAndSpill(state.join_keys, input, *sink.probe_spill,
		                                                      state.spill_state, state.spill_chunk);
#if RATCHET_PRINT == 1
        std::cout << "== state.spill ==" << std::endl;
        state.spill_chunk.Print();
        std::cout << "== input ==" << std::endl;
	    input.Print()
#endif
    } else {
		state.scan_structure = sink.hash_table->Probe(state.join_keys);
	}
    state.scan_structure->Next(state.join_keys, input, chunk);
    return OperatorResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
enum class HashJoinSourceStage : uint8_t { INIT, BUILD, PROBE, SCAN_HT, DONE };

class HashJoinLocalSourceState;

class HashJoinGlobalSourceState : public GlobalSourceState {
public:
	HashJoinGlobalSourceState(const PhysicalHashJoin &op, ClientContext &context);

	//! Initialize this source state using the info in the sink
	void Initialize(ClientContext &context, HashJoinGlobalSinkState &sink);
	//! Try to prepare the next stage
	void TryPrepareNextStage(HashJoinGlobalSinkState &sink);
	//! Prepare the next build/probe stage for external hash join (must hold lock)
	void PrepareBuild(HashJoinGlobalSinkState &sink);
	void PrepareProbe(HashJoinGlobalSinkState &sink);
	//! Assigns a task to a local source state
	bool AssignTask(HashJoinGlobalSinkState &sink, HashJoinLocalSourceState &lstate);

	idx_t MaxThreads() override {
		return probe_count / ((idx_t)STANDARD_VECTOR_SIZE * parallel_scan_chunk_count);
	}

public:
	const PhysicalHashJoin &op;

	//! For synchronizing the external hash join
	atomic<HashJoinSourceStage> global_stage;
	mutex lock;

	//! For HT build synchronization
	idx_t build_block_idx;
	idx_t build_block_count;
	idx_t build_block_done;
	idx_t build_blocks_per_thread;

	//! For probe synchronization
	idx_t probe_chunk_count;
	idx_t probe_chunk_done;

	//! For full/outer synchronization
	JoinHTScanState full_outer_scan;

	//! To determine the number of threads
	idx_t probe_count;
	idx_t parallel_scan_chunk_count;
};

class HashJoinLocalSourceState : public LocalSourceState {
public:
	HashJoinLocalSourceState(const PhysicalHashJoin &op, Allocator &allocator);

	//! Do the work this thread has been assigned
	void ExecuteTask(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, DataChunk &chunk);
	//! Whether this thread has finished the work it has been assigned
	bool TaskFinished();
	//! Build, probe and scan for external hash join
	void ExternalBuild(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate);
	void ExternalProbe(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, DataChunk &chunk);
	void ExternalScanHT(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate, DataChunk &chunk);

	//! Scans the HT for full/outer join
	void ScanFullOuter(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate);

public:
	//! The stage that this thread was assigned work for
	HashJoinSourceStage local_stage;
	//! Vector with pointers here so we don't have to re-initialize
	Vector addresses;

	//! Blocks assigned to this thread for building the pointer table
	idx_t build_block_idx_start;
	idx_t build_block_idx_end;

	//! Local scan state for probe spill
	ColumnDataConsumerScanState probe_local_scan;
	//! Chunks for holding the scanned probe collection
	DataChunk probe_chunk;
	DataChunk join_keys;
	DataChunk payload;
	//! Column indices to easily reference the join keys/payload columns in probe_chunk
	vector<idx_t> join_key_indices;
	vector<idx_t> payload_indices;
	//! Scan structure for the external probe
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;

	//! Current number of tuples from a full/outer scan that are 'in-flight'
	idx_t full_outer_found_entries;
	idx_t full_outer_in_progress;
};

unique_ptr<GlobalSourceState> PhysicalHashJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<HashJoinGlobalSourceState>(*this, context);
}

unique_ptr<LocalSourceState> PhysicalHashJoin::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_unique<HashJoinLocalSourceState>(*this, Allocator::Get(context.client));
}

HashJoinGlobalSourceState::HashJoinGlobalSourceState(const PhysicalHashJoin &op, ClientContext &context)
    : op(op), global_stage(HashJoinSourceStage::INIT), probe_chunk_count(0), probe_chunk_done(0),
      probe_count(op.children[0]->estimated_cardinality),
      parallel_scan_chunk_count(context.config.verify_parallelism ? 1 : 120) {
}

void HashJoinGlobalSourceState::Initialize(ClientContext &context, HashJoinGlobalSinkState &sink) {
	lock_guard<mutex> init_lock(lock);
	if (global_stage != HashJoinSourceStage::INIT) {
		// Another thread initialized
		return;
	}
	full_outer_scan.total = sink.hash_table->Count();

	idx_t num_blocks = sink.hash_table->GetBlockCollection().blocks.size();
	idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	build_blocks_per_thread = MaxValue<idx_t>((num_blocks + num_threads - 1) / num_threads, 1);

	// Finalize the probe spill too
	if (sink.probe_spill) {
		sink.probe_spill->Finalize();
	}

	global_stage = HashJoinSourceStage::PROBE;
}

void HashJoinGlobalSourceState::TryPrepareNextStage(HashJoinGlobalSinkState &sink) {
	lock_guard<mutex> guard(lock);
	switch (global_stage.load()) {
	case HashJoinSourceStage::BUILD:
		if (build_block_done == build_block_count) {
			sink.hash_table->finalized = true;
			PrepareProbe(sink);
		}
		break;
	case HashJoinSourceStage::PROBE:
		if (probe_chunk_done == probe_chunk_count) {
			if (IsRightOuterJoin(op.join_type)) {
				global_stage = HashJoinSourceStage::SCAN_HT;
			} else {
				PrepareBuild(sink);
			}
		}
		break;
	case HashJoinSourceStage::SCAN_HT:
		if (full_outer_scan.scanned == full_outer_scan.total) {
			PrepareBuild(sink);
		}
		break;
	default:
		break;
	}
}

void HashJoinGlobalSourceState::PrepareBuild(HashJoinGlobalSinkState &sink) {
	D_ASSERT(global_stage != HashJoinSourceStage::BUILD);
	auto &ht = *sink.hash_table;

	// Try to put the next partitions in the block collection of the HT
	if (!ht.PrepareExternalFinalize()) {
		global_stage = HashJoinSourceStage::DONE;
		return;
	}

	auto &block_collection = ht.GetBlockCollection();
	build_block_idx = 0;
	build_block_count = block_collection.blocks.size();
	build_block_done = 0;
	ht.InitializePointerTable();

	global_stage = HashJoinSourceStage::BUILD;
}

void HashJoinGlobalSourceState::PrepareProbe(HashJoinGlobalSinkState &sink) {
	sink.probe_spill->PrepareNextProbe();

	probe_chunk_count = sink.probe_spill->consumer->ChunkCount();
	probe_chunk_done = 0;

	if (IsRightOuterJoin(op.join_type)) {
		full_outer_scan.Reset();
		full_outer_scan.total = sink.hash_table->Count();
	}

	global_stage = HashJoinSourceStage::PROBE;
}

bool HashJoinGlobalSourceState::AssignTask(HashJoinGlobalSinkState &sink, HashJoinLocalSourceState &lstate) {
	D_ASSERT(lstate.TaskFinished());

	lock_guard<mutex> guard(lock);
	switch (global_stage.load()) {
	case HashJoinSourceStage::BUILD:
		if (build_block_idx != build_block_count) {
			lstate.local_stage = global_stage;
			lstate.build_block_idx_start = build_block_idx;
			build_block_idx = MinValue<idx_t>(build_block_count, build_block_idx + build_blocks_per_thread);
			lstate.build_block_idx_end = build_block_idx;
			return true;
		}
		break;
	case HashJoinSourceStage::PROBE:
		if (sink.probe_spill->consumer && sink.probe_spill->consumer->AssignChunk(lstate.probe_local_scan)) {
			lstate.local_stage = global_stage;
			return true;
		}
		break;
	case HashJoinSourceStage::SCAN_HT:
		if (full_outer_scan.scan_index != full_outer_scan.total) {
			lstate.local_stage = global_stage;
			lstate.ScanFullOuter(sink, *this);
			return true;
		}
		break;
	case HashJoinSourceStage::DONE:
		break;
	default:
		throw InternalException("Unexpected HashJoinSourceStage in AssignTask!");
	}
	return false;
}

HashJoinLocalSourceState::HashJoinLocalSourceState(const PhysicalHashJoin &op, Allocator &allocator)
    : local_stage(HashJoinSourceStage::INIT), addresses(LogicalType::POINTER) {
	auto &chunk_state = probe_local_scan.current_chunk_state;
	chunk_state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;

	auto &sink = (HashJoinGlobalSinkState &)*op.sink_state;
	probe_chunk.Initialize(allocator, sink.probe_types);
	join_keys.Initialize(allocator, op.condition_types);
	payload.Initialize(allocator, op.children[0]->types);

	// Store the indices of the columns to reference them easily
	idx_t col_idx = 0;
	for (; col_idx < op.condition_types.size(); col_idx++) {
		join_key_indices.push_back(col_idx);
	}
	for (; col_idx < sink.probe_types.size() - 1; col_idx++) {
		payload_indices.push_back(col_idx);
	}
}

void HashJoinLocalSourceState::ExecuteTask(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate,
                                           DataChunk &chunk) {
	switch (local_stage) {
	case HashJoinSourceStage::BUILD:
		ExternalBuild(sink, gstate);
		break;
	case HashJoinSourceStage::PROBE:
		ExternalProbe(sink, gstate, chunk);
		break;
	case HashJoinSourceStage::SCAN_HT:
		ExternalScanHT(sink, gstate, chunk);
		break;
	default:
		throw InternalException("Unexpected HashJoinSourceStage in ExecuteTask!");
	}
}

bool HashJoinLocalSourceState::TaskFinished() {
	switch (local_stage) {
	case HashJoinSourceStage::INIT:
	case HashJoinSourceStage::BUILD:
		return true;
	case HashJoinSourceStage::PROBE:
		return scan_structure == nullptr;
	case HashJoinSourceStage::SCAN_HT:
		return full_outer_in_progress == 0;
	default:
		throw InternalException("Unexpected HashJoinSourceStage in TaskFinished!");
	}
}

void HashJoinLocalSourceState::ExternalBuild(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate) {
	D_ASSERT(local_stage == HashJoinSourceStage::BUILD);

	auto &ht = *sink.hash_table;
	ht.Finalize(build_block_idx_start, build_block_idx_end, true);

	lock_guard<mutex> guard(gstate.lock);
	gstate.build_block_done += build_block_idx_end - build_block_idx_start;
}

void HashJoinLocalSourceState::ExternalProbe(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate,
                                             DataChunk &chunk) {
	D_ASSERT(local_stage == HashJoinSourceStage::PROBE && sink.hash_table->finalized);

	if (scan_structure) {
		// still have elements remaining (i.e. we got >STANDARD_VECTOR_SIZE elements in the previous probe)
		scan_structure->Next(join_keys, payload, chunk);
		if (chunk.size() == 0) {
			scan_structure = nullptr;
			sink.probe_spill->consumer->FinishChunk(probe_local_scan);
			lock_guard<mutex> lock(gstate.lock);
			gstate.probe_chunk_done++;
		}
		return;
	}

	// Scan input chunk for next probe
	sink.probe_spill->consumer->ScanChunk(probe_local_scan, probe_chunk);

	// Get the probe chunk columns/hashes
	join_keys.ReferenceColumns(probe_chunk, join_key_indices);
	payload.ReferenceColumns(probe_chunk, payload_indices);
	auto precomputed_hashes = &probe_chunk.data.back();

	// Perform the probe
	scan_structure = sink.hash_table->Probe(join_keys, precomputed_hashes);
	scan_structure->Next(join_keys, payload, chunk);
}

void HashJoinLocalSourceState::ExternalScanHT(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate,
                                              DataChunk &chunk) {
	D_ASSERT(local_stage == HashJoinSourceStage::SCAN_HT && full_outer_in_progress != 0);

	if (full_outer_found_entries != 0) {
		// Just did a scan, now gather
		sink.hash_table->GatherFullOuter(chunk, addresses, full_outer_found_entries);
		full_outer_found_entries = 0;
		return;
	}

	lock_guard<mutex> guard(gstate.lock);
	auto &fo_ss = gstate.full_outer_scan;
	fo_ss.scanned += full_outer_in_progress;
	full_outer_in_progress = 0;
}

void HashJoinLocalSourceState::ScanFullOuter(HashJoinGlobalSinkState &sink, HashJoinGlobalSourceState &gstate) {
	auto &fo_ss = gstate.full_outer_scan;
	idx_t scan_index_before = fo_ss.scan_index;
	full_outer_found_entries = sink.hash_table->ScanFullOuter(fo_ss, addresses);
	idx_t scanned = fo_ss.scan_index - scan_index_before;
	full_outer_in_progress = scanned;
}

void PhysicalHashJoin::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                               LocalSourceState &lstate_p) const {
#if RATCHET_PRINT == 1
    std::cout << "[PhysicalHashJoin::GetData] for pipeline " << context.pipeline->GetPipelineId() << std::endl;
#endif
    auto &sink = (HashJoinGlobalSinkState &)*sink_state;
	auto &gstate = (HashJoinGlobalSourceState &)gstate_p;
	auto &lstate = (HashJoinLocalSourceState &)lstate_p;
	sink.scanned_data = true;

	if (!sink.external) {
		if (IsRightOuterJoin(join_type)) {
			{
				lock_guard<mutex> guard(gstate.lock);
				lstate.ScanFullOuter(sink, gstate);
			}
			sink.hash_table->GatherFullOuter(chunk, lstate.addresses, lstate.full_outer_found_entries);
		}
		return;
	}

	D_ASSERT(can_go_external);
	if (gstate.global_stage == HashJoinSourceStage::INIT) {
		gstate.Initialize(context.client, sink);
	}

	// Any call to GetData must produce tuples, otherwise the pipeline executor thinks that we're done
	// Therefore, we loop until we've produced tuples, or until the operator is actually done
	while (gstate.global_stage != HashJoinSourceStage::DONE && chunk.size() == 0) {
		if (!lstate.TaskFinished() || gstate.AssignTask(sink, lstate)) {
			lstate.ExecuteTask(sink, gstate, chunk);
		} else {
			gstate.TryPrepareNextStage(sink);
		}
	}
}

} // namespace duckdb
