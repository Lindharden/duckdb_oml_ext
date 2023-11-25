#pragma once

#include "duckdb.hpp"

namespace duckdb {

class OmlExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

struct ReadOMLData : public TableFunctionData {
    unique_ptr<FileHandle> file_handle;                // Handle to the OML file
    bool file_end_reached;                             // Flag to indicate end of file
    idx_t row_count;                                   // Number of rows processed
    string schema;
    string table;
    string catalog;
    idx_t column_count;
    vector<LogicalType> column_types;
    vector<string> column_names;
    vector<bool> not_null_constraint;

    ReadOMLData() : file_end_reached(false), row_count(0) {
        schema = DEFAULT_SCHEMA;
        catalog = "memory";
    }
    
    // Additional state variables and methods can be added as needed
};

} // namespace duckdb
