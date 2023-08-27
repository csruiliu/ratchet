//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/constants.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <cstdint>
#include <vector>
#include <chrono>
#include "duckdb/common/string.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {

//! Enable Ratchet Printout
//! 0: no ratchet printout
//! 1: printout function invoking
//! 2: printout function invoking + query plan
#define RATCHET_PRINT 2

//! Ratchet Serialize and Deserialize Format
//! 0: CBOR
//! 1: JSON
#define RATCHET_SERDE_FORMAT 1

//! External Join
//! 0: Disable
//! 1: Enable
#define RATCHET_EXTERNAL_JOIN 0

// API versions
// if no explicit API version is defined, the latest API version is used
// Note that using older API versions (i.e. not using DUCKDB_API_LATEST) is deprecated.
// These will not be supported long-term, and will be removed in future versions.

#ifndef DUCKDB_API_0_3_1
#define DUCKDB_API_0_3_1 1
#endif
#ifndef DUCKDB_API_0_3_2
#define DUCKDB_API_0_3_2 2
#endif
#ifndef DUCKDB_API_LATEST
#define DUCKDB_API_LATEST DUCKDB_API_0_3_2
#endif

#ifndef DUCKDB_API_VERSION
#define DUCKDB_API_VERSION DUCKDB_API_LATEST
#endif

//! inline std directives that we use frequently
#ifndef DUCKDB_DEBUG_MOVE
using std::move;
#endif
using std::shared_ptr;
using std::unique_ptr;
using std::weak_ptr;
using data_ptr = unique_ptr<char[]>;
using std::make_shared;

// NOTE: there is a copy of this in the Postgres' parser grammar (gram.y)
#define DEFAULT_SCHEMA  "main"
#define INVALID_SCHEMA  ""
#define INVALID_CATALOG ""
#define SYSTEM_CATALOG  "system"
#define TEMP_CATALOG    "temp"

DUCKDB_API bool IsInvalidSchema(const string &str);
DUCKDB_API bool IsInvalidCatalog(const string &str);

//! a saner size_t for loop indices etc
typedef uint64_t idx_t;

//! The type used for row identifiers
typedef int64_t row_t;

//! The type used for hashes
typedef uint64_t hash_t;

//! data pointers
typedef uint8_t data_t;
typedef data_t *data_ptr_t;
typedef const data_t *const_data_ptr_t;

//! Type used for the selection vector
typedef uint32_t sel_t;
//! Type used for transaction timestamps
typedef idx_t transaction_t;

//! Type used for column identifiers
typedef idx_t column_t;
//! Type used for storage (column) identifiers
typedef idx_t storage_t;
//! Special value used to signify the ROW ID of a table
DUCKDB_API extern const column_t COLUMN_IDENTIFIER_ROW_ID;
DUCKDB_API bool IsRowIdColumnId(column_t column_id);

//! The maximum row identifier used in tables
extern const row_t MAX_ROW_ID;

extern const transaction_t TRANSACTION_ID_START;
extern const transaction_t MAX_TRANSACTION_ID;
extern const transaction_t MAXIMUM_QUERY_ID;
extern const transaction_t NOT_DELETED_ID;

extern const double PI;

//! global variable for Ratchet
// determine if the current process is for suspension or resumption
extern bool global_suspend;
extern bool global_resume;
// Suspend and resume file for in-memory operators
extern string global_suspend_file;
extern string global_resume_file;
// Suspend and resume folder for external operators
extern string global_suspend_folder;
extern string global_resume_folder;
// Time points and period to check if suspend should be triggered
extern std::chrono::steady_clock::time_point global_start;
extern uint64_t global_suspend_point_ms;
// It is for the cases where checking suspend and triggering suspend are in different functions
extern bool global_suspend_start;
// Records the ids of the pipelines that have been finalized
extern std::vector<uint16_t> global_finalized_pipelines;
// Indicates the id of the pipeline that should run when resuming
extern uint16_t global_resume_pipeline;
// Records the ids of the hashtable partitions
extern std::atomic<uint16_t> global_ht_partition;
// threads for resumption
extern uint16_t global_threads;
extern std::atomic<uint16_t> global_stopped_threads;

struct DConstants {
	//! The value used to signify an invalid index entry
	static constexpr const idx_t INVALID_INDEX = idx_t(-1);
};

struct Storage {
	//! The size of a hard disk sector, only really needed for Direct IO
	constexpr static int SECTOR_SIZE = 4096;
	//! Block header size for blocks written to the storage
	constexpr static int BLOCK_HEADER_SIZE = sizeof(uint64_t);
	// Size of a memory slot managed by the StorageManager. This is the quantum of allocation for Blocks on DuckDB. We
	// default to 256KB. (1 << 18)
	constexpr static int BLOCK_ALLOC_SIZE = 262144;
	//! The actual memory space that is available within the blocks
	constexpr static int BLOCK_SIZE = BLOCK_ALLOC_SIZE - BLOCK_HEADER_SIZE;
	//! The size of the headers. This should be small and written more or less atomically by the hard disk. We default
	//! to the page size, which is 4KB. (1 << 12)
	constexpr static int FILE_HEADER_SIZE = 4096;
};

struct LogicalIndex {
	explicit LogicalIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const LogicalIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator!=(const LogicalIndex &rhs) const {
		return index != rhs.index;
	};
	inline bool operator<(const LogicalIndex &rhs) const {
		return index < rhs.index;
	};
	bool IsValid() {
		return index != DConstants::INVALID_INDEX;
	}
};

struct PhysicalIndex {
	explicit PhysicalIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const PhysicalIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator!=(const PhysicalIndex &rhs) const {
		return index != rhs.index;
	};
	inline bool operator<(const PhysicalIndex &rhs) const {
		return index < rhs.index;
	};
	bool IsValid() {
		return index != DConstants::INVALID_INDEX;
	}
};

DUCKDB_API uint64_t NextPowerOfTwo(uint64_t v);

} // namespace duckdb
