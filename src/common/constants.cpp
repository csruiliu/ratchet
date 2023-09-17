#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

constexpr const idx_t DConstants::INVALID_INDEX;
const row_t MAX_ROW_ID = 4611686018427388000ULL; // 2^62
const column_t COLUMN_IDENTIFIER_ROW_ID = (column_t)-1;
const sel_t ZERO_VECTOR[STANDARD_VECTOR_SIZE] = {0};
const double PI = 3.141592653589793;

const transaction_t TRANSACTION_ID_START = 4611686018427388000ULL;                // 2^62
const transaction_t MAX_TRANSACTION_ID = NumericLimits<transaction_t>::Maximum(); // 2^63
const transaction_t NOT_DELETED_ID = NumericLimits<transaction_t>::Maximum() - 1; // 2^64 - 1
const transaction_t MAXIMUM_QUERY_ID = NumericLimits<transaction_t>::Maximum();   // 2^64

//! GLOBAL VARIABLE FOR RATCHET
//! Determine if the current process is for suspension or resumption
bool global_suspend = false;
bool global_resume = false;
//! Suspend and resume file for in-memory operators
string global_suspend_file = "sfile";
string global_resume_file = "rfile";
//! Suspend and resume folder for external operators
string global_suspend_folder = "sfolder";
string global_resume_folder = "rfolder";
//! Time points and period to check if suspend should be triggered
std::chrono::steady_clock::time_point global_start = {};
uint64_t global_suspend_point_ms = NumericLimits<transaction_t>::Maximum();
//! It is for the cases where checking suspend and triggering suspend are in different functions
bool global_suspend_start = false;
//! Records the ids of the pipelines that have been finalized
std::vector<uint16_t> global_finalized_pipelines;
//! Indicates the id of the pipeline that should run when resuming
uint16_t global_resume_pipeline = 0;
//! Records the ids of the hashtable partitions
atomic<uint16_t> global_ht_partition(0);
//! Threads for resumption
uint16_t global_threads = 0;
atomic<uint16_t> global_stopped_threads(0);
//! Flags for IPC
uint16_t shm_cost_model_flag = 0;
uint16_t shm_strategy = 0;
uint64_t shm_persistence_size = 0;
const char* shm_cost_model_flag_key = "/tmp/shm_cost_model_flag_key";
const char* shm_strategy_key = "/tmp/shm_strategy_key";
const char* shm_persistence_size_key = "/tmp/shm_persistence_size_key";


uint64_t NextPowerOfTwo(uint64_t v) {
	v--;
	v |= v >> 1;
	v |= v >> 2;
	v |= v >> 4;
	v |= v >> 8;
	v |= v >> 16;
	v |= v >> 32;
	v++;
	return v;
}

bool IsInvalidSchema(const string &str) {
	return str.empty();
}

bool IsInvalidCatalog(const string &str) {
	return str.empty();
}

bool IsRowIdColumnId(column_t column_id) {
	return column_id == COLUMN_IDENTIFIER_ROW_ID;
}

} // namespace duckdb
