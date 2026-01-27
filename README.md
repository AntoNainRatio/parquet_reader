# Parquet Reader

Parquet Reader is the work done during an Orange internship.
The goal was to to make it possible to read Parquet file by [**Khiops**](https://khiops.org/).

## Prerequisites

### CMake
You need to install [CMake](https://cmake.org/). I compiled this project using CMake on Visual Studio 2022.

## How it works

This repo is using [vcpkg](https://vcpkg.io/en/) to download the library [Arrow](https://arrow.apache.org/). We used it to read Parquet files different ways.

The repo contains 4 branches:
* *main*: It's the branch that should always contain the best working version of the code.
* *test*: Should be used to verify if the code is working or not before pushing it to main.
* *page_reader*: This branch contains a way of reading parquet by reading by column (Yes, the name is not that great...).
* *rg_reader*: This branch contains the other way of reading parquet by reading by row group.

You can read the report of this internship to get details of the implementations. Due to the size restrictions of the report, I couldn't explain everything in details.
That's why,we will go in detail of each of the both implementations by explaning what's not present in the report.

### Page Reader (page_reader)

The way its's working is by using the ParquetFile class using structures indexes. It's used to be able to know the logical position (position as CSV like reading) of all the structures present in the parquet file.

The `PageIndex` structure isn't necessary. It's still there because I didn't remove it but should have.
Currently we are using a PageIndex lists in the ColumnIndex but it only contains one PageIndex that contains all the values of the column.
The first plan was to use [PageReader](https://github.com/apache/arrow/blob/0d0e068da0904918e646f301fa75e75f66a6827b/cpp/src/parquet/page_index.cc#L383) to read the value where is the cursor.

But I didn't manage to make it work so I went for [columnReaders](https://github.com/apache/arrow/blob/main/cpp/src/parquet/column_reader.h).
This both classes are not documented and the only way to find how it works is to read the source code itself.

The actual implementation is handling almost every types but not all of them. We are not handling `INT96` and `FIXED_LEN_BYTE_ARRAY`. The reading of values is based on finding the corresponding value in C++.
I didn't find any int96_t values handled by C++, maybe there's some available. I didn't really dive into the `FIXED_LEN_BYTE_ARRAY`.

### Row Group Reader (rg_reader)



## Testing the code

### Testsuite

For testing purpose, I used `src/driver_test.cpp`. This is for testing the driver itself by calling the API.
The CMake configuration contains 2 build types for Windows and Linux. Those 2 types are release or debug.

### Scenarios

You can also test the driver with **Khiops**. I used scenarios that are in the folder `scnearios`. You can use them by using the **Khiops shell** this way:

```
khiops -i "path/to/the/scenario._kh" -e "path/to/the/log/file.txt"
```

You can create driver by using the Khiops application and finding the `khiops_data/lastrun/scenario._kh` in your user folder (It was my case).
