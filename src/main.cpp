#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <vector>
#include <chrono>

#include "parquet_file.h"
#include "khiopsdriver_file_parquet.h"

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <sys/stat.h>

static size_t get_file_size(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        return 0;
    }
    return st.st_size;
}

int read_whole_csv_file(const std::string path, bool timer) {
    size_t file_logical_size = get_file_size(path);
    std::cout << "File logical size: " << file_logical_size << std::endl;

    auto t1 = std::chrono::high_resolution_clock::now();

    FILE* f = fopen(path.c_str(), "rb");
    if (!f) {
        std::cerr << "Error: fopen failed\n";
        return 1;
    }

    size_t buffer_size = 10000;
    void* buf = calloc(1, buffer_size + 1);
    if (!buf) {
        std::cerr << "Error: calloc failed\n";
        fclose(f);
        return 1;
    }

    size_t curr = 0;
    while (curr < file_logical_size) {
        size_t read_bytes = fread(buf, 1, buffer_size, f);
        if (read_bytes == 0) {
            if (feof(f)) {
                break;
            }
            std::cerr << "Error: fread failed\n";
            free(buf);
            fclose(f);
            return 1;
        }

        curr += read_bytes;

        // ((char*)buf)[read_bytes] = 0;
        // std::cout << (char*)buf;
    }

    free(buf);

    if (fclose(f) != 0) {
        std::cerr << "Error closing file\n";
    }

    auto t2 = std::chrono::high_resolution_clock::now();

    if (timer) {
        std::cout << "CSV reading succeed in " << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << "millisecs" << std::endl;
    }

    return 0;
}


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

int read_whole_file(const std::string path, bool timer) {
    size_t file_logical_size = driver_getFileSize(path.c_str());
    std::cout << "File logical size: " << file_logical_size << std::endl;

    auto t1 = std::chrono::high_resolution_clock::now();

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
<<<<<<< HEAD
            std::cout << "Read bytes = " << code << std::endl;
            std::cout << "Total = " << curr << std::endl;
=======
           //std::cout << "Read bytes = " << code << "\n";
>>>>>>> main

           /* ((char*)buf)[code] = 0;
            std::cout << "Buffer contains: " << std::endl << (char*)buf << "<-EOF" << std::endl;

            std::cout << std::endl;*/

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

    auto t2 = std::chrono::high_resolution_clock::now();
    if (timer) {
        std::cout << "Parquet reading succeed in " << std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() << "millisecs" << std::endl;

    }

    return 0;
}

int compare() {
    const std::string csv = "C:/Users/Public/khiops_data/samples/AccidentsMedium/Places.txt";
    const std::string parquet = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";
    
    int error = 0;
    
    int code;
    code = read_whole_file(parquet, true);
    if (code != 0) {
        error++;
        std::cerr << "Error reading whole Parquet file." << std::endl;
        return -1;
    }

    code = read_whole_csv_file(csv, true);
    if (code != 0) {
        error++;
        std::cerr << "Error reading whole csv file." << std::endl;
        return -1;
    }
    return 0;
}

int main() {
    //const std::string path = "parquet://C/Users/KXFJ3896/Documents/parquet_reader/data/toto.parquet";
    //const std::string path = "parquet://C/Users/KXFJ3896/Documents/parquet_reader/data/hard.parquet";
<<<<<<< HEAD
=======
    //const std::string path = "parquet://C/Users/KXFJ3896/Documents/parquet_reader/data/test.parquet";
>>>>>>> main
    const std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";

    int error = 0;

    int code;
    // code = read_whole_file(path, true);
    code = compare();
    if (code != 0) {
        error++;
        std::cerr << "Error reading whole file." << std::endl;
    }

    return 0;
}
