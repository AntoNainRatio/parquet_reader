#include <stdio.h>
#include <iostream>

#include "parquet_file.h"

int main() {
	const std::string path = "C:/Users/KXFJ3896/Documents/parquet_reader/data/toto.parquet";
	ParquetFile parquet_file = ParquetFile(path); 

	return 0;
}