#include <stdio.h>
#include <iostream>

#include "parquet_file.h"
#include "khiopsdriver_file_parquet.h"

int main() {
	const std::string path = "C:/Users/KXFJ3896/Documents/parquet_reader/data/toto.parquet";

	
	auto driver = driver_fopen(path.c_str(), 'r');

	int code = 0;
	int size = 4;
	int count = 1;
	void* buf = calloc((count + 1), size);

	code = driver_fread(buf, size, count, driver);
	if (code != -1) {
		std::cout << "Buffer: " << (char*)buf << std::endl;

		std::cout << "code : " << code << std::endl;
	}
	else {
		std::cout << "driver_fread returned -1" << std::endl;
	}
	free(buf);	

	if (driver_fclose(driver) != 0) {
		std::cout << "Error closing driver" << std::endl;
	}


	return 0;
}