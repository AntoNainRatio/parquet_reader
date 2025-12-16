cmake_minimum_required(VERSION 3.20)
project(parquet_reader LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

add_library(khiopsdriver_file_parquet SHARED
            "src/khiopsdriver_file_parquet.h"    "src/khiopsdriver_file_parquet.cpp"
            "src/parquet_file.h"                 "src/parquet_file.cpp"
)

target_link_libraries(khiopsdriver_file_parquet 
    PRIVATE
        Arrow::arrow_shared
        Parquet::parquet_shared
)

set_target_properties(khiopsdriver_file_parquet PROPERTIES
    CXX_STANDARD 20
    PUBLIC_HEADER ${CMAKE_CURRENT_SOURCE_DIR}/khiopsdriver_file_parquet.h
    SOVERSION ${PROJECT_VERSION_MAJOR}
    VERSION ${PROJECT_VERSION}
)

if(WIN32)
  set_target_properties(khiopsdriver_file_parquet PROPERTIES
    WINDOWS_EXPORT_ALL_SYMBOLS ON
    RUNTIME_OUTPUT_NAME "libkhiopsdriver_file_parquet"
    ARCHIVE_OUTPUT_NAME "libkhiopsdriver_file_parquet"
  )
endif()

add_executable(parquet_reader_main "src/main.cpp")

target_link_libraries(parquet_reader_main
    PRIVATE
        Arrow::arrow_shared
        Parquet::parquet_shared
        khiopsdriver_file_parquet
)

add_executable(parquet_reader_test "src/driver_test.cpp")

target_link_libraries(parquet_reader_test
    PRIVATE
        Arrow::arrow_shared
        Parquet::parquet_shared
        khiopsdriver_file_parquet
)


