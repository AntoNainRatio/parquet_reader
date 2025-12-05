cmake_minimum_required(VERSION 3.20)
project(parquet_reader LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

add_executable(parquet_reader_main "src/main.cpp"
            "src/khiopsdriver_file_parquet.h"    "src/khiopsdriver_file_parquet.cpp"
            "src/parquet_file.h"                 "src/parquet_file.cpp")

target_link_libraries(parquet_reader_main
    PRIVATE
        Arrow::arrow_shared
        Parquet::parquet_shared
)
