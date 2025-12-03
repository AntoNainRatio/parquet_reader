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



struct ValueIndex {
    uint32_t row_index;             // index de la ligne dans le row group

    uint64_t value_logical_start;   // Position logique globale (en bytes)
    uint64_t value_logical_end;     // logical_offset_start + byte_len - 1

    uint32_t offset_in_page;        // offset dans la page décompressée
    uint32_t byte_len;              // taille brute de la valeur après décodage

};

struct PageIndex {
    uint32_t page_index;            // numéro de la page dans la colonne

    uint64_t page_logical_start;    // offset logique global du début de la page
    uint64_t page_logical_end;      // offset logique global de la fin de la page

    std::vector<ValueIndex> values; // index des valeurs présentes dans la page
};

struct ColumnIndex {
    int column_id;

    uint64_t column_logical_start;
    uint64_t column_logical_end;

    std::vector<PageIndex> pages;
};

struct RowGroupIndex {
    int row_group_id;

    uint64_t rowgroup_logical_start;
    uint64_t rowgroup_logical_end;

    std::vector<ColumnIndex> columns;
};


class ParquetFile {

    public:
        uint64_t pos = 0;               // logical current position
        uint64_t logical_size = 0;

		std::vector<RowGroupIndex> row_groups; // vector containing all metadata logical index

        std::unique_ptr<parquet::arrow::FileReader> reader;

        std::shared_ptr<parquet::FileMetaData> metadata;

        

    private:

        void BuildLogicalIndex()
        {
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


    public:
        ParquetFile(const std::string& path)
        {
            arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> result = arrow::io::ReadableFile::Open(path);
            if (!result.ok()) {
                throw std::runtime_error("Erreur lors de l'ouverture du fichier en lecture.");
            }

			std::shared_ptr<arrow::io::ReadableFile> infile = result.ValueOrDie();

            PARQUET_ASSIGN_OR_THROW(reader, parquet::arrow::OpenFile(infile, arrow::default_memory_pool()));

            metadata = reader->parquet_reader()->metadata();

            BuildLogicalIndex();
        }

        ~ParquetFile() {}

        void dumpInfo()
        {
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

};
