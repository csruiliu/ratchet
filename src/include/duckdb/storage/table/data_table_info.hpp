//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/data_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/storage/table/table_index_list.hpp"

namespace duckdb {
class DatabaseInstance;
class TableIOManager;

struct DataTableInfo {
	DataTableInfo(DatabaseInstance &db, shared_ptr<TableIOManager> table_io_manager_p, string schema, string table)
	    : db(db), table_io_manager(move(table_io_manager_p)), cardinality(0), schema(move(schema)), table(move(table)) {
	}

	//! The database instance of the table
	DatabaseInstance &db;
	//! The table IO manager
	shared_ptr<TableIOManager> table_io_manager;
	//! The amount of elements in the table. Note that this number signifies the amount of COMMITTED entries in the
	//! table. It can be inaccurate inside of transactions. More work is needed to properly support that.
	atomic<idx_t> cardinality;
	// schema of the table
	string schema;
	// name of the table
	string table;

	TableIndexList indexes;

	bool IsTemporary() {
		return schema == TEMP_SCHEMA;
	}
};

} // namespace duckdb
