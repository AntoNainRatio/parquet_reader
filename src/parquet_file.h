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

        void BuildLogicalIndex();


    public:
        ParquetFile(const std::string& path);

        ~ParquetFile();

        void dumpInfo();

        bool findValueAtLogicalPosition(int& out_row_group, 
                                int& out_column, 
                                int& out_page, 
			                    int& out_value);

        bool readValue(int rg,
                       int col,
                       int page,
                       int value,
                       std::vector<uint8_t>& out_bytes);
};
