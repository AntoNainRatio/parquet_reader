#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <vector>

#include "parquet_file.h"
#include "khiopsdriver_file_parquet.h"

void print_hex(const std::vector<uint8_t>& buf)
{
    std::cout << "HEX: ";
    for (size_t i = 0; i < buf.size(); ++i)
    {
        std::cout
            << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(buf[i]) << " ";
    }
    std::cout << std::dec << std::endl;
}

int read_some(int size, int count, void* driver) {
    size_t totalBytes = size * count;

    void* buf = calloc(1, totalBytes + 1);
    if (!buf) {
        std::cerr << "Error calloc\n";
        return 1;
    }

    long long code = driver_fread(buf, size, count, driver);

    if (code != -1) {
        std::cout << "Read bytes = " << code << "\n";

        std::vector<uint8_t> vec((uint8_t*)buf, (uint8_t*)buf + totalBytes);

        //print_hex(vec);
        std::cout << "Buffer contains: " << std::endl << (char*)buf << "<-EOF" << std::endl;

        std::cout << std::endl;
    }
    else {
        std::cerr << "driver_fread returned -1\n";
        free(buf);
        return 1;
    }

    free(buf);
    return 0;
}

int read_whole_file(const std::string path) {
    size_t file_logical_size = driver_getFileSize(path.c_str());
    std::cout << "File logical size: " << file_logical_size << std::endl;

    void* driver = driver_fopen(path.c_str(), 'r');
    if (!driver) {
        std::cerr << "Error: driver_fopen failed" << std::endl;
        return 1;
    }
    auto pd = (ParquetFile*)driver;
    //pd->dumpInfo();

    size_t buffer_size = 10000;
    void* buf = calloc(1, buffer_size + 1);
    if (!buf) {
        std::cerr << "Error calloc\n";
        return 1;
    }

    size_t curr = 0;
    while (curr < file_logical_size) {
        if (!buf) {
            std::cerr << "Error calloc\n";
            return 1;
        }

        long long code = driver_fread(buf, 1, buffer_size, driver);

        if (code != -1) {
            std::cout << "Read bytes = " << code << "\n";

            std::cout << "Buffer contains: " << std::endl << (char*)buf << "<-EOF" << std::endl;

            std::cout << std::endl;

            curr += code;
        }
        else {
            std::cerr << "driver_fread returned -1\n";
            free(buf);
            return 1;
        }
    }

    free(buf);

    if (driver_fclose(driver) != 0) {
        std::cerr << "Error closing driver\n";
    }

    return 0;
}

int main() {
    const std::string path = "parquet://C/Users/KXFJ3896/Documents/parquet_reader/data/toto.parquet";
    //const std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";

    int error = 0;

    int code;
    code = read_whole_file(path);
    if (code != 0) {
        error++;
        std::cerr << "Error reading whole file." << std::endl;
    }

    return 0;
}
