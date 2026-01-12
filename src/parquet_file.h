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

struct HeaderIndex {
    uint32_t col_index;

    uint64_t header_logical_start;
    uint64_t header_logical_end;
};


struct RowGroupIndex {
    int row_group_id;

    uint64_t rowgroup_logical_start;
    uint64_t rowgroup_logical_end;
};


class ParquetFile {

    public:
        uint64_t pos = 0;               // logical current position
        uint64_t logical_size = 0;

        std::vector<HeaderIndex> headers;
		std::vector<RowGroupIndex> row_groups; // vector containing all metadata logical index
        
        char sep;

        std::shared_ptr<parquet::arrow::FileReader> reader;

        std::shared_ptr<parquet::FileMetaData> metadata;

        

    private:

        void BuildLogicalIndex();
        bool is_open = false;


    public:
        ParquetFile(const std::string& path);

        ~ParquetFile();

		bool isOpen() const { return is_open; }

		void close() { is_open = false; }

        void dumpInfo();

        int32_t find_row_group(size_t& header);

        bool readHeader(size_t header, std::string& out_bytes);

        std::vector<std::shared_ptr<arrow::RecordBatch>> read(size_t row_group);
};
