#include "parquet_file.h"

#pragma once

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

void ParquetFile::BuildLogicalIndex() {
    if (!reader || !metadata) {
        throw std::runtime_error("Parquet reader or metadata not initialized");
    }
    std::cout << "Building logical index..." << std::endl;

    uint32_t num_row_groups = metadata->num_row_groups();
    uint32_t num_columns = metadata->num_columns();

    uint64_t global_offset = 0;
    row_groups.reserve(num_row_groups);

    auto parquet_reader = reader->parquet_reader();

    for (uint32_t rg = 0; rg < num_row_groups; rg++)
    {
        std::cout << "  Processing Row Group " << rg << " / " << num_row_groups - 1 << std::endl;

        RowGroupIndex rg_index;
        rg_index.row_group_id = rg;
        rg_index.rowgroup_logical_start = global_offset;
        rg_index.columns.resize(num_columns);

        std::shared_ptr<parquet::RowGroupReader> rg_reader =
            parquet_reader->RowGroup(rg);

        uint32_t num_rows = rg_reader->metadata()->num_rows();

        for (uint32_t col = 0; col < num_columns; col++)
        {
            std::cout << "    Processing Column " << col << " / " << num_columns - 1 << std::endl;

            ColumnIndex& c_index = rg_index.columns[col];
            c_index.column_id = col;
            c_index.column_logical_start = global_offset;

            std::shared_ptr<parquet::ColumnReader> col_reader =
                rg_reader->Column(col);

            uint32_t page_index = 0;
            uint32_t rows_read = 0;

            while (rows_read < num_rows)
            {
                std::cout << "      Processing Page " << page_index << std::endl;

                PageIndex page_idx;
                page_idx.page_index = page_index;
                page_idx.page_logical_start = global_offset;

                uint32_t batch_size = 1024 * 64; // "taille max" d'une page
                uint32_t total_values = 0;

                // buffers temporaires
                std::vector<int32_t> int_buffer;
                std::vector<int64_t> int64_buffer;
                std::vector<double> double_buffer;
                std::vector<float> float_buffer;

                std::vector<parquet::ByteArray> ba_buffer(batch_size);
                int64_t values_read = 0;

                parquet::Type::type physical =
                    metadata->schema()->Column(col)->physical_type();

                switch (physical)
                {
                case parquet::Type::INT32: {
                    std::cout << "        Reading INT32 values" << std::endl;
                    int_buffer.resize(batch_size);
                    auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::Int32Type>*>(col_reader.get());
                    total_values = typed->ReadBatch(
                        batch_size, nullptr, nullptr,
                        int_buffer.data(), &values_read);
                    std::cout << "        Read " << total_values << " INT32 values" << std::endl;
                    break;
                }

                case parquet::Type::INT64: {
                    std::cout << "        Reading INT64 values" << std::endl;
                    int64_buffer.resize(batch_size);
                    auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::Int64Type>*>(col_reader.get());
                    total_values = typed->ReadBatch(
                        batch_size, nullptr, nullptr,
                        int64_buffer.data(), &values_read);
                    std::cout << "        Read " << total_values << " INT64 values" << std::endl;
                    break;
                }

                case parquet::Type::FLOAT: {
                    std::cout << "        Reading FLOAT values" << std::endl;
                    float_buffer.resize(batch_size);
                    auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::FloatType>*>(col_reader.get());
                    total_values = typed->ReadBatch(
                        batch_size, nullptr, nullptr,
                        float_buffer.data(), &values_read);
                    std::cout << "        Read " << total_values << " FLOAT values" << std::endl;
                    break;
                }

                case parquet::Type::DOUBLE: {
                    std::cout << "        Reading DOUBLE values" << std::endl;
                    double_buffer.resize(batch_size);
                    auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::DoubleType>*>(col_reader.get());
                    total_values = typed->ReadBatch(
                        batch_size, nullptr, nullptr,
                        double_buffer.data(), &values_read);
                    std::cout << "        Read " << total_values << " DOUBLE values" << std::endl;
                    break;
                }

                case parquet::Type::BYTE_ARRAY:
                {
                    std::cout << "        Reading BYTE_ARRAY values" << std::endl;
                    auto* typed = dynamic_cast<parquet::TypedColumnReader<parquet::ByteArrayType>*>(col_reader.get());
                    total_values = typed->ReadBatch(batch_size, nullptr, nullptr, ba_buffer.data(), &values_read);

                    std::cout << "        Read " << total_values << " BYTE_ARRAY values" << std::endl;
                    break;
                }

                default:
                    throw std::runtime_error("Unsupported type");
                }

                if (values_read == 0)
                    break;

                // Values Indexing
                for (uint32_t i = 0; i < values_read; i++)
                {
                    std::cout << "        Processing Value " << (rows_read + i) << std::endl;

                    ValueIndex v;

                    v.row_index = rows_read + i;
                    v.offset_in_page = v.row_index;

                    switch (physical)
                    {
                    case parquet::Type::INT32:
                        v.byte_len = sizeof(int32_t);
                        break;
                    case parquet::Type::INT64:
                        v.byte_len = sizeof(int64_t);
                        break;
                    case parquet::Type::FLOAT:
                        v.byte_len = sizeof(float);
                        break;
                    case parquet::Type::DOUBLE:
                        v.byte_len = sizeof(double);
                        break;
                    case parquet::Type::BYTE_ARRAY:
                    {
                        parquet::ByteArray& ba = ba_buffer[i];
                        v.byte_len = ba.len;
                        break;
                    }
                    default:
                        v.byte_len = 0;
                    }

                    v.value_logical_start = global_offset;
                    v.value_logical_end = global_offset + v.byte_len - 1;

                    page_idx.values.push_back(v);

                    global_offset += v.byte_len;
                }

                // fin de la page
                page_idx.page_logical_end = global_offset - 1;
                c_index.pages.push_back(page_idx);

                rows_read += values_read;
                page_index++;
            }

            c_index.column_logical_end = global_offset - 1;
        }

        // fin row group
        rg_index.rowgroup_logical_end = global_offset - 1;
        row_groups.push_back(rg_index);
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
}

ParquetFile::~ParquetFile() {}

void ParquetFile::dumpInfo() {
    std::cout << "Dump of ParquetFile" << std::endl;
    std::cout << "logical size : " + logical_size << std::endl;
    std::cout << "logical pos : " + pos << std::endl;
    for (size_t rg = 0; rg < this->row_groups.size(); rg++) {
        RowGroupIndex rg_idx = row_groups[rg];

        for (size_t col = 0; col < rg_idx.columns.size(); col++) {
            ColumnIndex col_idx = rg_idx.columns[col];

            for (size_t page = 0; page < col_idx.pages.size(); page++) {
                PageIndex page_idx = col_idx.pages[page];

                for (size_t val = 0; val < page_idx.values.size(); val++) {
                    ValueIndex val_idx = page_idx.values[val];

                    std::cout << "            Dump of ValueIndex " << rg << "-" << col << "-" << page << "-" << val << std::endl;
                    std::cout << "            value row index in row group: " << val_idx.row_index << std::endl;
                    std::cout << "            value logical start: " << val_idx.value_logical_start << std::endl;
                    std::cout << "            value logical end: " << val_idx.value_logical_end << std::endl;
                    std::cout << "            value logical size: " << val_idx.byte_len << std::endl;
                    std::cout << "            -------------------" << std::endl;

                }
            }
        }
    }
}

bool ParquetFile::findValueAtLogicalPosition(int& out_row_group, int& out_column, int& out_page, int& out_value)
{
    for (size_t rg = 0; rg < this->row_groups.size(); rg++) {
        RowGroupIndex& rg_idx = row_groups[rg];
        if (this->pos < rg_idx.rowgroup_logical_start || this->pos > rg_idx.rowgroup_logical_end) {
            continue;
        }

        // found row group
        out_row_group = rg_idx.row_group_id;

        for (size_t col = 0; col < rg_idx.columns.size(); col++) {
            ColumnIndex& col_idx = rg_idx.columns[col];
            if (this->pos < col_idx.column_logical_start || this->pos > col_idx.column_logical_end) {
                continue;
            }

            // found column
            out_column = col_idx.column_id;

            for (size_t page = 0; page < col_idx.pages.size(); page++) {
                PageIndex& page_idx = col_idx.pages[page];
                if (this->pos < page_idx.page_logical_start || this->pos > page_idx.page_logical_end) {
                    continue;
                }

                // found page
                out_page = page_idx.page_index;

                for (size_t val = 0; val < page_idx.values.size(); val++) {
                    ValueIndex& val_idx = page_idx.values[val];
                    if (this->pos >= val_idx.value_logical_start && this->pos <= val_idx.value_logical_end) {
                        // found value
                        out_value = val;
                        return true;
                    }
                }
            }
        }
	}
    return false;
}

bool ParquetFile::readValue(int rg, int col, int page, int value, std::vector<uint8_t>& out_bytes)
{
    if (this->reader) return false;

    parquet::ParquetFileReader* pq_reader = this->reader->parquet_reader();
    if (!pq_reader) return false;

    auto rg_reader = pq_reader->RowGroup(rg);
    if (!rg_reader) return false;

    auto col_reader = rg_reader->Column(col);
    if (!col_reader) return false;

    parquet::Type::type phys = this->metadata->schema()->Column(col)->physical_type();

    ValueIndex to_read = this->row_groups[rg].columns[col].pages[page].values[value];

    int64_t to_skip = static_cast<int64_t>(to_read.row_index);

    int64_t values_read = 0;

    try {
        switch (phys) {
            case Type::INT32: {
                auto* typed = dynamic_cast<TypedColumnReader<parquet::Int32Type>*>(col_reader.get());
                if (!typed) return false;
                
                if (to_skip > 0) typed->Skip(to_skip);
                
                std::vector<int32_t> buf(1);
                int64_t read = typed->ReadBatch(1, nullptr, nullptr, buf.data(), &values_read);
                if (values_read <= 0) { // NULL or nothing
                    out_bytes.clear(); 
                    return true;
                } 
                out_bytes.resize(sizeof(int32_t));
                std::memcpy(out_bytes.data(), &buf[0], sizeof(int32_t));
                return true;
            }
            case Type::INT64: {
                auto* typed = dynamic_cast<TypedColumnReader<parquet::Int64Type>*>(col_reader.get());
                if (!typed) return false;
                
                if (to_skip > 0) typed->Skip(to_skip);

                std::vector<int64_t> buf(1);
                int64_t read = typed->ReadBatch(1, nullptr, nullptr, buf.data(), &values_read);
                if (values_read <= 0) {
                    out_bytes.clear();
                    return true;
                }
                out_bytes.resize(sizeof(int64_t));
                std::memcpy(out_bytes.data(), &buf[0], sizeof(int64_t));

                return true;
            }
            case Type::FLOAT: {
                auto* typed = dynamic_cast<TypedColumnReader<parquet::FloatType>*>(col_reader.get());
                if (!typed) return false;
                
                if (to_skip > 0) typed->Skip(to_skip);
                
                std::vector<float> buf(1);
                int64_t read = typed->ReadBatch(1, nullptr, nullptr, buf.data(), &values_read);
                if (values_read <= 0) {
                    out_bytes.clear();
                    return true;
                }
                out_bytes.resize(sizeof(float));
                std::memcpy(out_bytes.data(), &buf[0], sizeof(float));
                
                return true;
            }
            case Type::DOUBLE: {
                auto* typed = dynamic_cast<TypedColumnReader<parquet::DoubleType>*>(col_reader.get());
                if (!typed) return false;
                
                if (to_skip > 0) typed->Skip(to_skip);
                
                std::vector<double> buf(1);
                int64_t read = typed->ReadBatch(1, nullptr, nullptr, buf.data(), &values_read);
                if (values_read <= 0) {
                    out_bytes.clear();
                    return true;
                }
                out_bytes.resize(sizeof(double));
                std::memcpy(out_bytes.data(), &buf[0], sizeof(double));
               
                return true;
            }
            case Type::BYTE_ARRAY: {
                auto* typed = dynamic_cast<TypedColumnReader<parquet::ByteArrayType>*>(col_reader.get());
                if (!typed) return false;
                
                if (to_skip > 0) typed->Skip(to_skip);
                
                std::vector<parquet::ByteArray> buf(1);
                int64_t read = typed->ReadBatch(1, nullptr, nullptr, buf.data(), &values_read);
                if (values_read <= 0) {
                    out_bytes.clear();
                    return true;
                }
                
                // copy the bytes (no length prefix)
                out_bytes.resize(buf[0].len);
                if (buf[0].len > 0 && buf[0].ptr != nullptr)
                    std::memcpy(out_bytes.data(), buf[0].ptr, buf[0].len);

                return true;
            }
            default:
                // unsupported type
                return false;
        }
    }
    catch (...) {
        return false;
    }

    return false;
}

