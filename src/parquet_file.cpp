#pragma once

#include "parquet_file.h"

#include <memory>
#include <vector>
#include <cstdint>
#include <iostream>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/api/reader.h>

using parquet::TypedColumnReader;
using parquet::Type;

char separator = '\t';

void ParquetFile::BuildLogicalIndex() {
    if (!reader || !metadata)
        throw std::runtime_error("Parquet reader or metadata not initialized");

    uint64_t global_offset = 0;

    uint32_t num_row_groups = metadata->num_row_groups();
    uint32_t num_columns = metadata->num_columns();

    sep = separator;

    headers.clear();
    headers.reserve(num_columns);

    const parquet::SchemaDescriptor* schema = metadata->schema();

    for (uint32_t i = 0; i < num_columns; ++i) {
        const parquet::ColumnDescriptor* col = schema->Column(i);
        const auto& path = col->path()->ToDotString();
        
        HeaderIndex header_idx;
        header_idx.col_index = i;
        header_idx.header_logical_start = global_offset;

        global_offset += path.size() + 1;

        header_idx.header_logical_end = global_offset - 1;
        headers.push_back(header_idx);
    }

    row_groups.clear();
    row_groups.reserve(num_row_groups);

    auto parquet_reader = reader->parquet_reader();

    for (uint32_t rg = 0; rg < num_row_groups; rg++)
    {
        RowGroupIndex rg_idx;
        rg_idx.row_group_id = rg;
        rg_idx.rowgroup_logical_start = global_offset;

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches = read(rg);

        size_t col = 0;
        size_t row = 0;
        size_t batch_id = 0;

        while (batch_id < batches.size())
        {
            std::shared_ptr<arrow::RecordBatch> batch = batches[batch_id];
            std::shared_ptr<arrow::Array> array = batch->column(col);

            std::string value = array->GetScalar(row)->get()->ToString();

            bool need_quote = false;
            for (size_t i = 0; i < value.size(); i++) {
                char c = value[i];
                if (value[i] == sep || value[i] == '"' || value[i] == '\n') {
                    need_quote = true;
                    break;
                }
            }
            if (need_quote) {
                global_offset += 2;             // both '"'
                global_offset += std::ranges::count(value, '"');     // inside '"' are doubled
            }
            global_offset += value.size() + 1;  // value's size and newline or sep everytime


            col++;
            if (col >= batch->num_columns()) {
                row++;
                col = 0;
            }
            if (row >= batch->num_rows()) {
                batch_id++;
                row = 0;
            }
        }

        rg_idx.rowgroup_logical_end = global_offset;

        row_groups.push_back(rg_idx);
    }

    logical_size = global_offset;
}




ParquetFile::ParquetFile(const std::string& path) {
    arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> result = arrow::io::ReadableFile::Open(path);
    if (!result.ok()) {
        throw std::runtime_error("Erreur lors de l'ouverture du fichier en lecture.");
    }

    std::shared_ptr<arrow::io::ReadableFile> infile = result.ValueOrDie();

    PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));

    metadata = reader->parquet_reader()->metadata();

    BuildLogicalIndex();

	is_open = true;
}

ParquetFile::~ParquetFile() {}

void ParquetFile::dumpInfo() {
    std::cout << "Dump of ParquetFile" << std::endl;
    std::cout << "logical size : " + logical_size << std::endl;
    std::cout << "logical pos : " + pos << std::endl;
    for (size_t rg = 0; rg < this->row_groups.size(); rg++) {
        RowGroupIndex rg_idx = row_groups[rg];

        std::cout << "row group " << rg << ": start = " << rg_idx.rowgroup_logical_start <<
            ", end = " << rg_idx.rowgroup_logical_end << std::endl;
    }
}

int32_t ParquetFile::find_row_group(size_t& header) {
    for (size_t i = 0; i < headers.size(); i++) {
        if (headers[i].header_logical_start <= pos && headers[i].header_logical_end > pos) {
            header = i;
            return -1;
        }
    }
    for (int32_t i = 0; i < row_groups.size(); i++) {
        if (row_groups[i].rowgroup_logical_start <= pos && row_groups[i].rowgroup_logical_end > pos) {
            return i;
        }
    }
    return -1;
}

bool ParquetFile::readHeader(size_t header, std::string& out_bytes) {
    const parquet::ColumnDescriptor* col = this->metadata->schema()->Column(header);
    std::string value = col->path()->ToDotString();
    if (header == this->headers.size() - 1) {
        value.push_back('\n');
    }
    else {
        value.push_back(sep);
    }
    out_bytes = value;
    return true;
}


std::vector<std::shared_ptr<arrow::RecordBatch>> ParquetFile::read(size_t row_group)
{
    std::vector<int> row_groups = { (int)row_group };

    std::shared_ptr<arrow::RecordBatchReader> batch_reader;
    
    PARQUET_ASSIGN_OR_THROW(
        batch_reader,
        reader->GetRecordBatchReader(row_groups)
    );

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        PARQUET_ASSIGN_OR_THROW(batch, batch_reader->Next());
        if (!batch) break;
        batches.push_back(batch);
    }

    return batches;
}

