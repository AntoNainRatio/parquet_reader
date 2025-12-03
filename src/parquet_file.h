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
                        uint32_t values_read = 0;

                        // buffers temporaires
                        std::vector<int32_t> int_buffer;
                        std::vector<int64_t> int64_buffer;
                        std::vector<double> double_buffer;
                        std::vector<float> float_buffer;

                        parquet::Type::type physical =
                            metadata->schema()->Column(col)->physical_type();

                        switch (physical)
                        {
                        case parquet::Type::INT32: {
							std::cout << "        Reading INT32 values" << std::endl;
                            int_buffer.resize(batch_size);
                            auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::Int32Type>*>(col_reader.get());
                            values_read = typed->ReadBatch(
                                batch_size, nullptr, nullptr,
                                int_buffer.data(), nullptr);
							std::cout << "        Read " << values_read << " INT32 values" << std::endl;
                            break;
                        }

                        case parquet::Type::INT64: {
							std::cout << "        Reading INT64 values" << std::endl;
                            int64_buffer.resize(batch_size);
                            auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::Int64Type>*>(col_reader.get());
                            values_read = typed->ReadBatch(
                                batch_size, nullptr, nullptr,
                                int64_buffer.data(), nullptr);
                            break;
                        }

                        case parquet::Type::FLOAT: {
							std::cout << "        Reading FLOAT values" << std::endl;
                            float_buffer.resize(batch_size);
                            auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::FloatType>*>(col_reader.get());
                            values_read = typed->ReadBatch(
                                batch_size, nullptr, nullptr,
                                float_buffer.data(), nullptr);
                            break;
                        }

                        case parquet::Type::DOUBLE: {
							std::cout << "        Reading DOUBLE values" << std::endl;
                            double_buffer.resize(batch_size);
                            auto typed = dynamic_cast<parquet::TypedColumnReader<parquet::DoubleType>*>(col_reader.get());
                            values_read = typed->ReadBatch(
                                batch_size, nullptr, nullptr,
                                double_buffer.data(), nullptr);
                            break;
                        }

                        case parquet::Type::BYTE_ARRAY:
                        {
							std::cout << "        Reading BYTE_ARRAY values" << std::endl;
                            std::vector<parquet::ByteArray> ba_buffer(batch_size);
                            auto* typed = dynamic_cast<parquet::TypedColumnReader<parquet::ByteArrayType>*>(col_reader.get());
                            values_read = typed->ReadBatch(batch_size, nullptr, nullptr, ba_buffer.data(), nullptr);

                            for (uint32_t i = 0; i < values_read; i++)
                            {
                                ValueIndex v;
                                v.row_index = rows_read + i;
                                v.offset_in_page = v.row_index;
                                v.byte_len = ba_buffer[i].len;
                                v.value_logical_start = global_offset;
                                v.value_logical_end = global_offset + v.byte_len - 1;

                                page_idx.values.push_back(v);

                                global_offset += v.byte_len;
                            }
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
                            v.offset_in_page = v.row_index; // pour colonnes fixes : index → offset naturel

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

                rg_index.rowgroup_logical_end = global_offset - 1;
                row_groups.push_back(rg_index);
            }

            logical_size = global_offset;
        }


        //uint64_t EstimateLogicalPageSize(const parquet::Page& page,
        //    uint32_t column_id)
        //{
        //    auto schema = metadata->schema()->Column(column_id);
        //    parquet::Type::type t = schema->physical_type();

        //    uint32_t num_values = page.num_values();

        //    switch (t) {
        //    case parquet::Type::INT32:
        //        return num_values * sizeof(int32_t);

        //    case parquet::Type::INT64:
        //        return num_values * sizeof(int64_t);

        //    case parquet::Type::FLOAT:
        //        return num_values * sizeof(float);

        //    case parquet::Type::DOUBLE:
        //        return num_values * sizeof(double);

        //    case parquet::Type::BYTE_ARRAY:
        //        return EstimateByteArrayLogicalSize(page);

        //    default:
        //        // TODO: autres types si besoin
        //        return num_values * 8;
        //    }
        //}

        //uint64_t EstimateByteArrayLogicalSize(const parquet::Page& page)
        //{
        //    // TODO: Decode page to get the size of the string
        //    return page.num_values() * 8;
        //}

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

};
