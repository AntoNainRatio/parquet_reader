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

int main() {
    const std::string path = "C:/Users/KXFJ3896/Documents/parquet_reader/data/toto.parquet";

    size_t file_logical_size = driver_getFileSize(path.c_str());
    std::cout << "File logical size: " << file_logical_size << std::endl;

    void* driver = driver_fopen(path.c_str(), 'r');
    if (!driver) {
        std::cerr << "Error: driver_fopen failed" << std::endl;
        return 1;
    }
    auto pd = (ParquetFile*)driver;
    //pd->dumpInfo();

    //for (int i = 0; i < 7; i++) {
        read_some(1, file_logical_size, driver);
    //}

    if (driver_fclose(driver) != 0) {
        std::cerr << "Error closing driver\n";
    }

    return 0;
}
