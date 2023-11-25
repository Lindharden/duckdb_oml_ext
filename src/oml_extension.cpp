#define DUCKDB_EXTENSION_MAIN

#include "oml_extension.hpp"
#include <fstream>
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include <cctype>
#include <iostream>

namespace duckdb {

LogicalType stringToLogicalType(const std::string &type) {
    if (type == "string") {
        return LogicalType::VARCHAR;
    } else if (type == "uint32") {
        return LogicalType::INTEGER;
    } else if (type == "int32") {
        return LogicalType::INTEGER;
    } else if (type == "double" || type == "float") {
        return LogicalType::FLOAT;
    }
    throw InternalException("Unknown type: " + type);
}

Value StringLogicalTypeToValue(const std::string &parsed_val, LogicalType &type) {
    if (type == LogicalType::VARCHAR) {
        return Value(parsed_val);
    } else if (type == LogicalType::FLOAT || type == LogicalType::DOUBLE) {
        return Value::FLOAT((std::stof(parsed_val)));
    } else if (type == LogicalType::INTEGER) {
        return Value::INTEGER((std::stoi(parsed_val)));
    }
    throw InternalException("Invalid type: " + parsed_val + " " + type.ToString());
}

void CreateTable(ClientContext &context, ReadOMLData &bind_data, Catalog &catalog) {
    auto table_info = make_uniq<CreateTableInfo>();
    table_info->schema = bind_data.schema;
    table_info->table = bind_data.table;
    table_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;

    // Define the schema for the oml table from ReadOMLData
    for (idx_t i = 0; i < bind_data.column_count; i++) {
        table_info->columns.AddColumn(ColumnDefinition(bind_data.column_names[i], bind_data.column_types[i]));
        if (!bind_data.not_null_constraint.empty() && bind_data.not_null_constraint[i]) {
            table_info->constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(i)));
        }
    }

    catalog.CreateTable(context, std::move(table_info));
}

void CreateSequence(ClientContext &context, ReadOMLData &bind_data, Catalog &catalog) {
    auto seq_info = make_uniq<CreateSequenceInfo>();
    seq_info->schema = bind_data.schema;
    seq_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
    seq_info->name = "Power_Consumption_id_seq";
    seq_info->usage_count = UINT64_MAX;
    seq_info->increment = 1;
    seq_info->min_value = 0;
    seq_info->max_value = INT64_MAX; 
    seq_info->start_value = 0;

    catalog.CreateSequence(context, *seq_info);
}

void CreateView(ClientContext &context, ReadOMLData &bind_data, Catalog &catalog) {
    auto view_info = make_uniq<CreateViewInfo>();
    view_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
    view_info->view_name = "PC";

    string sql =
        "CREATE OR REPLACE VIEW PC AS (SELECT nextval('Power_Consumption_id_seq') AS id, "
        "cast(time_sec AS real) + cast(time_usec AS real) AS ts, "
        "power, current, voltage "
        "FROM Power_Consumption);";

    view_info = view_info->FromCreateView(context, sql);
    view_info->schema = bind_data.schema;

    catalog.CreateView(context, *view_info);
}

static unique_ptr<FunctionData> PowerConsumptionLoadBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
    if (input.inputs.size() != 1 || input.inputs[0].type() != LogicalType::VARCHAR) {
        throw std::runtime_error("OML function requires a single VARCHAR argument (filepath)");
    }
    
    auto bind_data = make_uniq<ReadOMLData>();
    
    auto &fs = duckdb::FileSystem::GetFileSystem(context);
    auto file_path = input.inputs[0].GetValue<string>();
    auto file_handle = fs.OpenFile(file_path, duckdb::FileFlags::FILE_FLAGS_READ);
    bind_data->file_handle = move(file_handle);
    bind_data->schema = DEFAULT_SCHEMA;
    bind_data->table = "Power_Consumption";
    bind_data->catalog = "memory";
    bind_data->column_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::FLOAT, LogicalType::FLOAT, LogicalType::FLOAT};
    bind_data->column_names = {"experiment_id", "node_id", "node_id_seq", "time_sec", "time_usec", "power", "current", "voltage"};
    bind_data->not_null_constraint = {false, false, false, true, true, true, true, true};
    bind_data->column_count = bind_data->column_types.size();

    // Output - row count
    return_types.emplace_back(LogicalType::INTEGER);
    names = {"row_count"};
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("Power_Consumption");

    Catalog &catalog = Catalog::GetCatalog(context, bind_data->catalog);
    CreateTable(context, *bind_data, catalog);
    CreateSequence(context, *bind_data, catalog);
    CreateView(context, *bind_data, catalog);

    return std::move(bind_data);
}


static unique_ptr<FunctionData> OmlLoadBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
    if (input.inputs.size() != 1 || input.inputs[0].type() != LogicalType::VARCHAR) {
        throw std::runtime_error("OML function requires a single VARCHAR argument (filepath)");
    }
    
    auto bind_data = make_uniq<ReadOMLData>();
    
    auto &fs = duckdb::FileSystem::GetFileSystem(context);
    auto file_path = input.inputs[0].GetValue<string>();
    auto file_handle = fs.OpenFile(file_path, duckdb::FileFlags::FILE_FLAGS_READ);

    std::string line;
    for (int i = 0; i < 8; ++i) {
        line = file_handle->ReadLine();

        std::istringstream iss(line);
        std::string token;
        iss >> token;

        if (token == "schema:") {
            std::string column_info;
            while (iss >> column_info) {
                size_t colon_pos = column_info.find(':');
                if (colon_pos != std::string::npos) {
                    std::string column_name = column_info.substr(0, colon_pos);
                    std::string column_type = column_info.substr(colon_pos + 1);
                    bind_data->column_names.push_back(column_name);
                    bind_data->column_types.push_back(stringToLogicalType(column_type));
                } else {
                    bind_data->table = column_info;
                }
            }
        }
    }

    file_handle->Reset();
    bind_data->file_handle = std::move(file_handle);
    bind_data->column_count = bind_data->column_names.size();
    bind_data->not_null_constraint = {};

    // Output - row count
    return_types.emplace_back(LogicalType::INTEGER);
    names.emplace_back("row_count");    
    return_types.emplace_back(LogicalType::VARCHAR);
    names.emplace_back("table_name");

    auto &catalog = Catalog::GetCatalog(context, bind_data->catalog);
    CreateTable(context, *bind_data, catalog);

    return std::move(bind_data);
}

static void AppendValues(InternalAppender *appender, vector<Value> values) {
    appender->BeginRow();
    for (size_t i = 0; i < values.size(); ++i) {
        appender->Append(Value(values[i]));
    }
    appender->EndRow();
}

static void OmlLoadFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = (ReadOMLData &)*data.bind_data;

    if (state.file_end_reached) {
        return;
    }

    auto &catalog = Catalog::GetCatalog(context, state.catalog);
    auto &table_catalog_entry = catalog.GetEntry<TableCatalogEntry>(context, state.schema, state.table);
    auto appender = make_uniq<duckdb::InternalAppender>(context, table_catalog_entry);

    std::string line;
    state.row_count = 0;
    state.file_handle->Reset();

    while (true) {
        line = state.file_handle->ReadLine();
        if (state.file_handle->SeekPosition() >= state.file_handle->GetFileSize()) {
            // End of file reached, break the loop
            state.file_end_reached = true;
            break;
        }

        // Skip metadata lines or empty lines
        size_t firstNonWhitespace = line.find_first_not_of(" \t");
        if (line.empty() || firstNonWhitespace == std::string::npos || (!std::isdigit(line[firstNonWhitespace]) && line[firstNonWhitespace] != '.')) {
            continue;
        }

        std::istringstream iss(line);
        vector<string> values;
        string value;
        while (iss >> value) {
            values.push_back(value);
        }

        // Handle parsing error
        if (values.size() != state.column_count) {
            throw std::runtime_error("Error parsing line: " + line);
        }

        vector<duckdb::Value> data;
        // convert parsed value to duckdb::Value
        for (idx_t i = 0; i < state.column_count; i++) {
            data.push_back(StringLogicalTypeToValue(values[i], state.column_types[i]));
        }

        AppendValues(appender.get(), data);

        state.row_count++;
    }

    state.file_end_reached = true;
    state.file_handle->Reset();
    appender->Close();

    output.SetValue(0, 0, Value::INTEGER(state.row_count));
    output.SetValue(1, 0, Value(state.table));
    output.SetCardinality(1);
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register OML table functions
    auto oml_load_power_consumption = TableFunction("Power_Consumption_load", {LogicalType::VARCHAR}, OmlLoadFunction, PowerConsumptionLoadBind);
    ExtensionUtil::RegisterFunction(instance, oml_load_power_consumption);

    auto oml_load = TableFunction("OmlGen", {LogicalType::VARCHAR}, OmlLoadFunction, OmlLoadBind);
    ExtensionUtil::RegisterFunction(instance, oml_load);
}

void OmlExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string OmlExtension::Name() {
	return "oml";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void oml_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *oml_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

