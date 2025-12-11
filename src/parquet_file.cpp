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

char sep = ',';

void ParquetFile::BuildLogicalIndex() {
    if (!reader || !metadata)
        throw std::runtime_error("Parquet reader or metadata not initialized");

    uint32_t num_row_groups = metadata->num_row_groups();
    uint32_t num_columns = metadata->num_columns();

    uint64_t global_offset = 0;
    row_groups.clear();
    row_groups.reserve(num_row_groups);

    auto parquet_reader = reader->parquet_reader();

    for (uint32_t rg = 0; rg < num_row_groups; rg++)
    {
        RowGroupIndex rg_idx;
        rg_idx.row_group_id = rg;
        rg_idx.rowgroup_logical_start = global_offset;
        rg_idx.columns.resize(num_columns);

        auto rg_reader = parquet_reader->RowGroup(rg);
        uint32_t num_rows = rg_reader->metadata()->num_rows();

        // Ouvrir tous les ColumnReader en avance
        std::vector<std::shared_ptr<parquet::ColumnReader>> col_readers(num_columns);
        for (uint32_t col = 0; col < num_columns; col++) {
            col_readers[col] = rg_reader->Column(col);
            rg_idx.columns[col].column_id = col;
            rg_idx.columns[col].column_logical_start = global_offset;

            // Initialiser une page vide
            PageIndex first_page;
            first_page.page_index = 0;
            first_page.page_logical_start = global_offset;
            rg_idx.columns[col].pages.push_back(first_page);
        }

        // Lecture en mode row-major
        for (uint32_t row = 0; row < num_rows; row++)
        {
            for (uint32_t col = 0; col < num_columns; col++)
            {
                auto phys = metadata->schema()->Column(col)->physical_type();
                auto& col_reader = col_readers[col];

                ValueIndex v;
                v.row_index = row;

                std::vector<uint8_t> tmp;

                // ---- Lire UNE SEULE VALEUR par colonne ----
                int64_t values_read = 0;

                switch (phys)
                {
                case parquet::Type::INT32: {
                    int32_t val;
                    auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::Int32Type>*>(col_reader.get());
                    typed->ReadBatch(1, nullptr, nullptr, &val, &values_read);
                    std::string s = std::to_string(val);
                    v.byte_len = s.size() + 1;
                    break;
                }

                case parquet::Type::INT64: {
                    int64_t val;
                    auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::Int64Type>*>(col_reader.get());
                    typed->ReadBatch(1, nullptr, nullptr, &val, &values_read);
                    std::string s = std::to_string(val);
                    v.byte_len = s.size() + 1;
                    break;
                }

                case parquet::Type::FLOAT: {
                    float val;
                    auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::FloatType>*>(col_reader.get());
                    typed->ReadBatch(1, nullptr, nullptr, &val, &values_read);
                    std::string s = std::to_string(val);
                    v.byte_len = s.size() + 1;
                    break;
                }

                case parquet::Type::DOUBLE: {
                    double val;
                    auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::DoubleType>*>(col_reader.get());
                    typed->ReadBatch(1, nullptr, nullptr, &val, &values_read);
                    std::string s = std::to_string(val);
                    v.byte_len = s.size() + 1;
                    break;
                }

                case parquet::Type::BYTE_ARRAY: {
                    parquet::ByteArray ba;
                    auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::ByteArrayType>*>(col_reader.get());
                    typed->ReadBatch(1, nullptr, nullptr, &ba, &values_read);
                    v.byte_len = ba.len + 1;
                    break;
                }

                /*case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
                    int32_t type_len = metadata->schema()->Column(col)->type_length();
                    parquet::FixedLenByteArray flba;
                    auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::FixedLenByteArrayType>*>(col_reader.get());
                    typed->ReadBatch(1, nullptr, nullptr, &flba, &values_read);
                    v.byte_len = type_len;
                    tmp.resize(v.byte_len);
                    memcpy(tmp.data(), flba.ptr, type_len);
                    break;
                }*/

                default:
                    throw std::runtime_error("Unsupported type");
                }

                // Indexation logique
                v.value_logical_start = global_offset;
                v.value_logical_end = global_offset + v.byte_len - 1;

                global_offset += v.byte_len;

                // Ajouter dans la seule page existante
                auto& col_idx = rg_idx.columns[col];
                col_idx.pages.back().values.push_back(v);
            }
        }

        // Finaliser fin de pages et colonnes
        for (uint32_t col = 0; col < num_columns; col++)
        {
            auto& c = rg_idx.columns[col];
            c.pages.back().page_logical_end = global_offset - 1;
            c.column_logical_end = global_offset - 1;
        }

        rg_idx.rowgroup_logical_end = global_offset - 1;
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

                    std::cout << "            Dump of ValueIndex: (rg: " << rg << ")-(col: " << col << ")-(page: " << page << ")-(val: " << val << ")" << std::endl;
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

bool ParquetFile::findValueAtLogicalPosition(size_t& out_row_group, size_t& out_column, size_t& out_page, size_t& out_value)
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
    if (!this->reader) return false;

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
            case Type::BOOLEAN: {
                auto* typed = dynamic_cast<TypedColumnReader<parquet::BooleanType>*>(col_reader.get());
                if (!typed) return false;
                if (to_skip > 0) typed->Skip(to_skip);
                bool val;
                int64_t read = typed->ReadBatch(1, nullptr, nullptr, &val, &values_read);
                if (values_read <= 0) { // NULL or nothing
                    out_bytes.clear();
                    return true;
                }
                std::string s = std::to_string(val);
                out_bytes.resize(s.size());
                std::memcpy(out_bytes.data(), s.data(), s.size());
                if (col == this->metadata->num_columns() - 1) {
                    out_bytes.push_back('\n');
                }
                else {
                    out_bytes.push_back(sep);
                }

                return true;
			}
            case Type::INT32: {
                auto* typed = dynamic_cast<TypedColumnReader<parquet::Int32Type>*>(col_reader.get());
                if (!typed) return false;
                
                if (to_skip > 0) typed->Skip(to_skip);
                
                int32_t val;
                int64_t read = typed->ReadBatch(1, nullptr, nullptr, &val, &values_read);
                if (values_read <= 0) { // NULL or nothing
                    out_bytes.clear(); 
                    return true;
                }
                std::string s = std::to_string(val);
                out_bytes.resize(s.size());
                std::memcpy(out_bytes.data(), s.data(), s.size());
                if (col == this->metadata->num_columns() - 1) {
                    out_bytes.push_back('\n');
                }
                else {
                    out_bytes.push_back(sep);
                }

                return true;
            }
            case Type::INT64: {
                auto* typed = dynamic_cast<TypedColumnReader<parquet::Int64Type>*>(col_reader.get());
                if (!typed) return false;
                
                if (to_skip > 0) typed->Skip(to_skip);

                int64_t val;
                int64_t read = typed->ReadBatch(1, nullptr, nullptr, &val, &values_read);
                if (values_read <= 0) {
                    out_bytes.clear();
                    return true;
                }
                std::string s = std::to_string(val);
                out_bytes.resize(s.size());
                std::memcpy(out_bytes.data(), s.data(), s.size());
                if (col == this->metadata->num_columns() - 1) {
                    out_bytes.push_back('\n');
                }
                else {
                    out_bytes.push_back(sep);
                }

                return true;
            }
            /*case Type::INT96: {
                auto* typed = dynamic_cast<TypedColumnReader<parquet::Int96Type>*>(col_reader.get());
                if (!typed) return false;
                
                if (to_skip > 0) typed->Skip(to_skip);
                
                parquet::Int96 val;
                int64_t read = typed->ReadBatch(1, nullptr, nullptr, &val, &values_read);
                if (values_read <= 0) {
                    out_bytes.clear();
                    return true;
                }
                std::string s = std::to_string(val).append(sep);
                out_bytes.resize(s.size());
                std::memcpy(out_bytes.data(), s.data(), s.size());
                
                return true;
			}*/
            case Type::FLOAT: {
                auto* typed = dynamic_cast<TypedColumnReader<parquet::FloatType>*>(col_reader.get());
                if (!typed) return false;
                
                if (to_skip > 0) typed->Skip(to_skip);
                
                float val;
                int64_t read = typed->ReadBatch(1, nullptr, nullptr, &val, &values_read);
                if (values_read <= 0) {
                    out_bytes.clear();
                    return true;
                }
                std::string s = std::to_string(val);
                out_bytes.resize(s.size());
                std::memcpy(out_bytes.data(), s.data(), s.size());
                if (col == this->metadata->num_columns() - 1) {
                    out_bytes.push_back('\n');
                }
                else {
                    out_bytes.push_back(sep);
                }
                
                return true;
            }
            case Type::DOUBLE: {
                auto* typed = dynamic_cast<TypedColumnReader<parquet::DoubleType>*>(col_reader.get());
                if (!typed) return false;
                
                if (to_skip > 0) typed->Skip(to_skip);
                
                double val;
                int64_t read = typed->ReadBatch(1, nullptr, nullptr, &val, &values_read);
                if (values_read <= 0) {
                    out_bytes.clear();
                    return true;
                }
                std::string s = std::to_string(val);
                out_bytes.resize(s.size());
                std::memcpy(out_bytes.data(), s.data(), s.size());
                if (col == this->metadata->num_columns() - 1) {
                    out_bytes.push_back('\n');
                }
                else {
                    out_bytes.push_back(sep);
                }

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
                    if (col == this->metadata->num_columns() - 1) {
                        out_bytes.push_back('\n');
                    }
                    else{
                        out_bytes.push_back(sep);
                    }

                return true;
            }
            /*case Type::FIXED_LEN_BYTE_ARRAY: {
                auto* typed = dynamic_cast<TypedColumnReader<parquet::ByteArrayType>*>(col_reader.get());
                if (!typed) return false;
                
                if (to_skip > 0) typed->Skip(to_skip);
                
                int32_t type_len = this->metadata->schema()->Column(col)->type_length();
                
                std::vector<parquet::FixedLenByteArray> buf(1);
                int64_t read = typed->ReadBatch(1, nullptr, nullptr, buf.data(), &values_read);
                if (values_read <= 0) {
                    out_bytes.clear();
                    return true;
                }
                
                out_bytes.resize(type_len);
                if (type_len > 0 && buf[0].ptr != nullptr)
                    std::memcpy(out_bytes.data(), buf[0].ptr, type_len);
                return true;
			}*/
            default:
                // unsupported type
                std::cout << "ParquetFile::ReadValue: Unsupported type" << std::endl;
                return false;
        }
    }
    catch (...) {
        return false;
    }

    return false;
}

