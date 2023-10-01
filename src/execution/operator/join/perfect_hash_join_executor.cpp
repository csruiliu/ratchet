#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"

#include "duckdb/common/types/row_layout.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"

namespace duckdb {

PerfectHashJoinExecutor::PerfectHashJoinExecutor(const PhysicalHashJoin &join_p, JoinHashTable &ht_p,
                                                 PerfectHashJoinStats perfect_join_stats)
    : join(join_p), ht(ht_p), perfect_join_statistics(std::move(perfect_join_stats)) {
}

bool PerfectHashJoinExecutor::CanDoPerfectHashJoin() {
	return perfect_join_statistics.is_build_small;
}

//===--------------------------------------------------------------------===//
// Build
//===--------------------------------------------------------------------===//
bool PerfectHashJoinExecutor::BuildPerfectHashTable(LogicalType &key_type) {
	// First, allocate memory for each build column
	auto build_size = perfect_join_statistics.build_range + 1;
	for (const auto &build_type : ht.build_types) {
		perfect_hash_table.emplace_back(build_type, build_size);
	}

    // allocate memory for each join key column
    for (const auto &condition_type : ht.condition_types) {
        join_keys_perfect_hash_table.emplace_back(condition_type, build_size);
    }

	// and for duplicate_checking
	bitmap_build_idx = unique_ptr<bool[]>(new bool[build_size]);
	memset(bitmap_build_idx.get(), 0, sizeof(bool) * build_size); // set false

	// pin all fixed-size blocks (variable-sized should still be pinned)
	ht.PinAllBlocks();

	// Now fill columns with build data
	JoinHTScanState join_ht_state;
	return FullScanHashTable(join_ht_state, key_type);
}

bool PerfectHashJoinExecutor::FullScanHashTable(JoinHTScanState &state, LogicalType &key_type) {
	Vector tuples_addresses(LogicalType::POINTER, ht.Count());              // allocate space for all the tuples
	auto key_locations = FlatVector::GetData<data_ptr_t>(tuples_addresses); // get a pointer to vector data
	// TODO: In a parallel finalize: One should exclusively lock and each thread should do one part of the code below.
	// Go through all the blocks and fill the keys addresses
	auto keys_count = ht.FillWithHTOffsets(key_locations, state);
	// Scan the build keys in the hash table
	Vector build_vector(key_type, keys_count);
	RowOperations::FullScanColumn(ht.layout, tuples_addresses, build_vector, keys_count, 0);
	// Now fill the selection vector using the build keys and create a sequential vector
	// todo: add check for fast pass when probe is part of build domain
	SelectionVector sel_build(keys_count + 1);
	SelectionVector sel_tuples(keys_count + 1);
	bool success = FillSelectionVectorSwitchBuild(build_vector, sel_build, sel_tuples, keys_count);
	// early out
	if (!success) {
		return false;
	}
	if (unique_keys == perfect_join_statistics.build_range + 1 && !ht.has_null) {
		perfect_join_statistics.is_build_dense = true;
	}
	keys_count = unique_keys; // do not consider keys out of the range
	// Full scan the remaining build columns and fill the perfect hash table
	for (idx_t i = 0; i < ht.build_types.size(); i++) {
		auto build_size = perfect_join_statistics.build_range + 1;
		auto &vector = perfect_hash_table[i];
		D_ASSERT(vector.GetType() == ht.build_types[i]);
		const auto col_no = ht.condition_types.size() + i;
		RowOperations::Gather(tuples_addresses, sel_tuples, vector, sel_build, keys_count, ht.layout, col_no,
		                      build_size);
	}
    // Fill the join key column
    for (idx_t i = 0; i < ht.condition_types.size(); i++) {
        auto build_size = perfect_join_statistics.build_range + 1;
        auto &join_keys_vector = join_keys_perfect_hash_table[i];
        D_ASSERT(join_keys_vector.GetType() == ht.condition_types[i]);
        const auto col_no = i;
        RowOperations::Gather(tuples_addresses, sel_tuples, join_keys_vector, sel_build, keys_count, ht.layout, col_no,
                              build_size);
    }
	return true;
}

bool PerfectHashJoinExecutor::FillSelectionVectorSwitchBuild(Vector &source, SelectionVector &sel_vec,
                                                             SelectionVector &seq_sel_vec, idx_t count) {
	switch (source.GetType().InternalType()) {
	case PhysicalType::INT8:
		return TemplatedFillSelectionVectorBuild<int8_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT16:
		return TemplatedFillSelectionVectorBuild<int16_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT32:
		return TemplatedFillSelectionVectorBuild<int32_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::INT64:
		return TemplatedFillSelectionVectorBuild<int64_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT8:
		return TemplatedFillSelectionVectorBuild<uint8_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT16:
		return TemplatedFillSelectionVectorBuild<uint16_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT32:
		return TemplatedFillSelectionVectorBuild<uint32_t>(source, sel_vec, seq_sel_vec, count);
	case PhysicalType::UINT64:
		return TemplatedFillSelectionVectorBuild<uint64_t>(source, sel_vec, seq_sel_vec, count);
	default:
		throw NotImplementedException("Type not supported for perfect hash join");
	}
}

template <typename T>
bool PerfectHashJoinExecutor::TemplatedFillSelectionVectorBuild(Vector &source, SelectionVector &sel_vec,
                                                                SelectionVector &seq_sel_vec, idx_t count) {
	if (perfect_join_statistics.build_min.IsNull() || perfect_join_statistics.build_max.IsNull()) {
		return false;
	}
	auto min_value = perfect_join_statistics.build_min.GetValueUnsafe<T>();
	auto max_value = perfect_join_statistics.build_max.GetValueUnsafe<T>();
	UnifiedVectorFormat vector_data;
	source.ToUnifiedFormat(count, vector_data);
	auto data = reinterpret_cast<T *>(vector_data.data);
	// generate the selection vector
	for (idx_t i = 0, sel_idx = 0; i < count; ++i) {
		auto data_idx = vector_data.sel->get_index(i);
		auto input_value = data[data_idx];
		// add index to selection vector if value in the range
		if (min_value <= input_value && input_value <= max_value) {
			auto idx = (idx_t)(input_value - min_value); // subtract min value to get the idx position
			sel_vec.set_index(sel_idx, idx);
			if (bitmap_build_idx[idx]) {
				return false;
			} else {
				bitmap_build_idx[idx] = true;
				unique_keys++;
			}
			seq_sel_vec.set_index(sel_idx++, i);
		}
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Probe
//===--------------------------------------------------------------------===//
class PerfectHashJoinState : public OperatorState {
public:
	PerfectHashJoinState(ClientContext &context, const PhysicalHashJoin &join) : probe_executor(context) {
		join_keys.Initialize(Allocator::Get(context), join.condition_types);
		for (auto &cond : join.conditions) {
			probe_executor.AddExpression(*cond.left);
		}
		build_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
		probe_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
		seq_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
	}

	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	SelectionVector build_sel_vec;
	SelectionVector probe_sel_vec;
	SelectionVector seq_sel_vec;
};

unique_ptr<OperatorState> PerfectHashJoinExecutor::GetOperatorState(ExecutionContext &context) {
	auto state = make_unique<PerfectHashJoinState>(context.client, join);
	return std::move(state);
}

OperatorResultType PerfectHashJoinExecutor::ProbePerfectHashTable(ExecutionContext &context, DataChunk &input,
                                                                  DataChunk &result, OperatorState &state_p) {
#if RATCHET_PRINT >= 1
    std::cout << "[PerfectHashJoinExecutor::ProbePerfectHashTable] for pipeline " << context.pipeline->GetPipelineId() << std::endl;
#endif
    auto &state = (PerfectHashJoinState &)state_p;
	// keeps track of how many probe keys have a match
	idx_t probe_sel_count = 0;

	// fetch the join keys from the chunk
	state.join_keys.Reset();
	state.probe_executor.Execute(input, state.join_keys);
	// select the keys that are in the min-max range
	auto &keys_vec = state.join_keys.data[0];
	auto keys_count = state.join_keys.size();

    // todo: add check for fast pass when probe is part of build domain
    FillSelectionVectorSwitchProbe(keys_vec, state.build_sel_vec, state.probe_sel_vec, keys_count, probe_sel_count);

    // If build is dense and probe is in build's domain, just reference probe
    if (perfect_join_statistics.is_build_dense && keys_count == probe_sel_count) {
        result.Reference(input);
    } else {
        // otherwise, filter it out the values that do not match
        result.Slice(input, state.probe_sel_vec, probe_sel_count, 0);
    }

    // on the build side, we need to fetch the data and build dictionary vectors with the sel_vec
    for (idx_t i = 0; i < ht.build_types.size(); i++) {
        auto &result_vector = result.data[input.ColumnCount() + i];
        D_ASSERT(result_vector.GetType() == ht.build_types[i]);
        auto &build_vec = perfect_hash_table[i];
        result_vector.Reference(build_vec);
        result_vector.Slice(state.build_sel_vec, probe_sel_count);
    }

	return OperatorResultType::NEED_MORE_INPUT;
}

void PerfectHashJoinExecutor::FillSelectionVectorSwitchProbe(Vector &source, SelectionVector &build_sel_vec,
                                                             SelectionVector &probe_sel_vec, idx_t count,
                                                             idx_t &probe_sel_count) {
	switch (source.GetType().InternalType()) {
	case PhysicalType::INT8:
		TemplatedFillSelectionVectorProbe<int8_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::INT16:
		TemplatedFillSelectionVectorProbe<int16_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::INT32:
		TemplatedFillSelectionVectorProbe<int32_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::INT64:
		TemplatedFillSelectionVectorProbe<int64_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::UINT8:
		TemplatedFillSelectionVectorProbe<uint8_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::UINT16:
		TemplatedFillSelectionVectorProbe<uint16_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::UINT32:
		TemplatedFillSelectionVectorProbe<uint32_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	case PhysicalType::UINT64:
		TemplatedFillSelectionVectorProbe<uint64_t>(source, build_sel_vec, probe_sel_vec, count, probe_sel_count);
		break;
	default:
		throw NotImplementedException("Type not supported");
	}
}

template <typename T>
void PerfectHashJoinExecutor::TemplatedFillSelectionVectorProbe(Vector &source, SelectionVector &build_sel_vec,
                                                                SelectionVector &probe_sel_vec, idx_t count,
                                                                idx_t &probe_sel_count) {
	auto min_value = perfect_join_statistics.build_min.GetValueUnsafe<T>();
	auto max_value = perfect_join_statistics.build_max.GetValueUnsafe<T>();

	UnifiedVectorFormat vector_data;
	source.ToUnifiedFormat(count, vector_data);
	auto data = reinterpret_cast<T *>(vector_data.data);
	auto validity_mask = &vector_data.validity;
	// build selection vector for non-dense build
	if (validity_mask->AllValid()) {
		for (idx_t i = 0, sel_idx = 0; i < count; ++i) {
			// retrieve value from vector
			auto data_idx = vector_data.sel->get_index(i);
			auto input_value = data[data_idx];
			// add index to selection vector if value in the range
			if (min_value <= input_value && input_value <= max_value) {
				auto idx = (idx_t)(input_value - min_value); // subtract min value to get the idx position
				                                             // check for matches in the build
				if (bitmap_build_idx[idx]) {
					build_sel_vec.set_index(sel_idx, idx);
					probe_sel_vec.set_index(sel_idx++, i);
					probe_sel_count++;
				}
			}
		}
	} else {
		for (idx_t i = 0, sel_idx = 0; i < count; ++i) {
			// retrieve value from vector
			auto data_idx = vector_data.sel->get_index(i);
			if (!validity_mask->RowIsValid(data_idx)) {
				continue;
			}
			auto input_value = data[data_idx];
			// add index to selection vector if value in the range
			if (min_value <= input_value && input_value <= max_value) {
				auto idx = (idx_t)(input_value - min_value); // subtract min value to get the idx position
				                                             // check for matches in the build
				if (bitmap_build_idx[idx]) {
					build_sel_vec.set_index(sel_idx, idx);
					probe_sel_vec.set_index(sel_idx++, i);
					probe_sel_count++;
				}
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// Ratchet
//===--------------------------------------------------------------------===//
void PerfectHashJoinExecutor::SerializePerfectHashTable() {
    std::cout << "== Serialize PerfectHashTable ==" << std::endl;

    json json_data;

    auto build_size = perfect_join_statistics.build_range + 1;

    //! TODO: handle ht.build_types.size() != ht.condition_types.size()
    D_ASSERT(ht.build_types.size() == ht.condition_types.size());
    json_data["pipeline_complete"] = global_finalized_pipelines;
    json_data["column_size"] = ht.build_types.size();
    json_data["build_size"] = build_size;

    for (idx_t i = 0; i < ht.build_types.size(); i++) {
        auto &build_vec = perfect_hash_table[i];
        // std::cout << ht.build_types.at(i).ToString() << std::endl;
        if (ht.build_types.at(i) == LogicalType::VARCHAR) {
            vector<string> value_vector;
            for (idx_t j = 0; j < build_size; j++) {
                value_vector.push_back(build_vec.GetValue(j).ToString());
            }
            json_data["build_chunk_" + to_string(i)]["type"] = LogicalType::VARCHAR;
            json_data["build_chunk_" + to_string(i)]["data"] = value_vector;
        } else if (ht.build_types.at(i) == LogicalType::INTEGER) {
            vector<int64_t> value_vector;
            for (idx_t j = 0; j < build_size; j++) {
                value_vector.push_back(build_vec.GetValue(j).ToInt64());
            }
            json_data["build_chunk_" + to_string(i)]["type"] = LogicalType::INTEGER;
            json_data["build_chunk_" + to_string(i)]["data"] = value_vector;
        } else if (ht.build_types.at(i) == LogicalType::DOUBLE) {
            vector<double_t> value_vector;
            for (idx_t j = 0; j < build_size; j++) {
                value_vector.push_back(build_vec.GetValue(j).ToDouble());
            }
            json_data["build_chunk_" + to_string(i)]["type"] = LogicalType::DOUBLE;
            json_data["build_chunk_" + to_string(i)]["data"] = value_vector;
        } else if (ht.build_types.at(i) == LogicalType::DATE) {
            vector<string> value_vector;
            for (idx_t j = 0; j < build_size; j++) {
                value_vector.push_back(build_vec.GetValue(j).ToString());
            }
            json_data["build_chunk_" + to_string(i)]["type"] = LogicalType::DATE;
            json_data["build_chunk_" + to_string(i)]["data"] = value_vector;
        } else {
            throw ParserException("Cannot recognize build types");
        }
    }

    for (idx_t i = 0; i < ht.condition_types.size(); i++) {
        auto &key_vec = join_keys_perfect_hash_table[i];

        if (ht.condition_types.at(i) == LogicalType::VARCHAR) {
            vector<string> key_vector;
            for (idx_t j = 0; j < build_size; j++) {
                key_vector.push_back(key_vec.GetValue(j).ToString());
            }
            json_data["join_key_" + to_string(i)]["type"] = LogicalType::VARCHAR;
            json_data["join_key_" + to_string(i)]["data"] = key_vector;
        } else if (ht.condition_types.at(i) == LogicalType::INTEGER) {
            vector<int64_t> key_vector;
            for (idx_t j = 0; j < build_size; j++) {
                key_vector.push_back(key_vec.GetValue(j).ToInt64());
            }
            json_data["join_key_" + to_string(i)]["type"] = LogicalType::INTEGER;
            json_data["join_key_" + to_string(i)]["data"] = key_vector;
        } else {
            throw ParserException("Cannot recognize key types");
        }
    }



#if RATCHET_SERDE_FORMAT == 0
    std::ofstream outputFile(global_suspend_file, std::ios::out | std::ios::binary);
    const auto output_vector = json::to_cbor(json_data);
    std::cout << "Estimated Persistence Size in CBOR (bytes): " << output_vector.size() * sizeof(uint8_t) << std::endl;
    outputFile.write(reinterpret_cast<const char *>(output_vector.data()), output_vector.size());
#elif RATCHET_SERDE_FORMAT == 1
    std::ofstream outputFile(global_suspend_file);
    outputFile << json_data;
#endif
    outputFile.close();
    if (outputFile.fail()) {
        std::cerr << "Error writing to file!" << std::endl;
    }
}

} // namespace duckdb
