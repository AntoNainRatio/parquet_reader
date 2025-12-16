#include <stdio.h>
#include <iostream>
#include <iomanip>
#include <vector>

#include "parquet_file.h"
#include "khiopsdriver_file_parquet.h"

#define VERBOSE false

int test_driver_fopen_errors() {
	std::vector<const char*> args = { nullptr, "non_existent" };

	int failed = 0;

	for (const char* arg : args) {
		ParquetFile* mf = (ParquetFile*)driver_fopen(arg, 'r');
		if (mf != nullptr) {
			failed++;
		}
	}

	return failed;
}

int test_driver_fclose_erros() {
	std::vector<void*> args = { nullptr /*, (void*)"non_existent" , (void*)2*/ };

	int failed = 0;

	for (void* arg : args) {
		int code = driver_fclose(arg);
		if (code != EOF) {
			failed++;
		}
	}

	return failed;
}

int test_driver_fread_errors() {
	int failed = 0;

	int code;

	std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";

	// opening file
	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');
	if (mf == nullptr) {
		throw std::runtime_error("driver_fopen error during driver_fread errors.");
	}

	// init buffer
	const size_t buffer_size = 1024;
	char* buffer = (char*)malloc(buffer_size * sizeof(char));

	size_t size = sizeof(char);
	size_t count = 500;

	code = driver_fread(NULL, size, count, mf);
	if (code != -1) {
		std::cout << "driver_fread errors: NULL ptr doesn't return -1" << std::endl;
		failed++;
	}

	code = driver_fread(buffer, size, count, NULL);
	if (code != -1) {
		std::cout << "driver_fread errors: NULL ParquetFile doesn't return -1" << std::endl;
		failed++;
	}



	code = driver_fclose(mf);
	if (code == -1) {
		throw std::runtime_error("driver_close error during driver_fread tests.");
	}

	free(buffer);
	return failed;
}

// multiple fread call (2 errors test and 1 valid test)
int test_driver_fread() {
	int failed = 0;

	int code;

	std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";

	// opening file
	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');
	if (mf == nullptr) {
		throw std::runtime_error("driver_fopen error during driver_fread.");
	}

	// init buffer
	const size_t buffer_size = 1024;
	char* buffer = (char*)malloc(buffer_size * sizeof(char));

	size_t size = sizeof(char);
	size_t count = 500;

	code = driver_fread(buffer, 0, count, mf);
	if (code != 0) {
		std::cout << "driver_fread errors: size=0 doesn't return 0" << std::endl;
		failed++;
	}

	code = driver_fread(buffer, size, 0, mf);
	if (code != 0) {
		std::cout << "driver_fread errors: count=0 doesn't return 0" << std::endl;
		failed++;
	}

	code = driver_fclose(mf);
	if (code == -1) {
		throw std::runtime_error("driver_close error during driver_fread tests.");
	}

	free(buffer);
	return failed;
}

// read all file from begin to end using driver_fread
int test_driver_fread_all_file() {
	std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";

	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');
	if (mf == nullptr) {
		throw std::runtime_error("driver_fopen error during driver_fread all file test.");
	}
	int code;

	const size_t buffer_size = 60000;
	char* buffer = (char*)calloc(buffer_size, sizeof(char));

	size_t total_read = 0;

	size_t total_read_target = driver_getFileSize(path.c_str());
	// std::cout << "size : " << total_read_target << " bytes" << std::endl;
	size_t read_size_in_loop = buffer_size;
	while (total_read < total_read_target) {
		/*std::cout << "BEGGING OF LOOP" << std::endl;
		std::cout << "fread call" << std::endl;*/
		code = driver_fread(buffer, sizeof(char), read_size_in_loop, mf);
		// std::cout << "end of fread call" << std::endl;

		if (code != -1) {
			// std::cout << "Read " << code << " bytes." << std::endl;
			total_read += code;
		}
		else {
			std::cout << "driver_fread all file test: error reading the whole file." << std::endl;
			return 1;
		}
		// std::cout << "END of loop" << std::endl;
	}

	if (total_read != total_read_target) {
		std::cout << "driver_fread all file test: read more than there is in file." << std::endl;
		return 1;
	}

	code = driver_fclose(mf);
	if (code == -1) {
		throw std::runtime_error("driver_close error during driver_fread all file test.");
	}

	free(buffer);
	return 0;
}

int test_driver_fread_whole_file_in_one_read() {
	int code;

	std::string path = "parquet://C/Users/KXFJ3896/Documents/parquet_reader/data/toto.parquet";

	// opening file
	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');
	if (mf == nullptr) {
		throw std::runtime_error("driver_fopen error during driver_fread tests.");
	}

	// init buffer
	const size_t buffer_size = 1024;
	char* buffer = (char*)calloc(buffer_size, 1);

	size_t size = 1;
	size_t count = 500;

	code = driver_fread(buffer, size, count, mf);
	if (code == -1) {
		std::cout << "driver_fread tests error: unable to read" << std::endl;
		free(buffer);
		return 1;
	}

	const char* exp = "1,USA,New York\n2,Canada,Toronto\n3,UK,London\n4,Australia,Sydney\n5,Germany,Berlin\n6,France,Paris\n7,Japan,Tokyo\n8,Spain,Madrid\n";
	if (strcmp(buffer, exp) != 0) {
		std::cout << "driver_fread tests error: invalid buffer content (exp: \"" << exp << "\", got: \"" << buffer << "\")" << std::endl;
		free(buffer);
		return 1;
	}

	code = driver_fread(buffer, size, count, mf);
	if (code == -1 || code != 0) {
		std::cout << "driver_fread tests error: unable to read or read to much" << std::endl;
		free(buffer);
		return 1;
	}

	free(buffer);

	return 0;
}

// testing error handling in driver_fseek function
int test_driver_fseek_errors() {
	int failed = 0;

	int code;
	code = driver_fseek(NULL, 10000, std::ios::beg);
	if (code != -1) {
		std::cout << "driver_fseek errors: NULL ParquetFile doesn't return -1." << std::endl;
		failed++;
	}

	std::string path = "parquet://C/Users/KXFJ3896/Documents/parquet_reader/data/toto.parquet";

	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');
	if (mf == nullptr) {
		throw std::runtime_error("driver_fopen error during driver_fseek errors.");
	}

	size_t file_size = driver_getFileSize(path.c_str());
	code = driver_fseek(mf, file_size + 1, std::ios::beg);
	if (code != -1) {
		std::cout << "driver_fseek errors: file_size+1 offset from BEGIN doesn't return -1." << std::endl;
		failed++;
	}
	code = driver_fseek(mf, file_size + 1, std::ios::cur);
	if (code != -1) {
		std::cout << "driver_fseek errors: file_size+1 offset from CURRENT(BEGIN) doesn't return -1." << std::endl;
		failed++;
	}

	code = driver_fseek(mf, -10000, std::ios::cur);
	if (code != -1) {
		std::cout << "driver_fseek errors: negative offset from CURRENT(BEGIN) doesn't return -1." << std::endl;
		failed++;
	}

	code = driver_fseek(mf, 10000, std::ios::end);
	if (code != -1) {
		std::cout << "driver_fseek errors: positive offset from END doesn't return -1." << std::endl;
		failed++;
	}

	code = driver_fseek(mf, -10000, std::ios::end);
	if (code != -1) {
		std::cout << "driver_fseek errors: negative offset from BEGIN doesn't return -1." << std::endl;
		failed++;
	}

	code = driver_fseek(mf, file_size + 1, std::ios::cur);
	if (code != -1) {
		std::cout << "driver_fseek errors: (filesize + 1) offset from CURRENT doesn't return -1." << std::endl;
		failed++;
	}

	code = driver_fseek(mf, -(file_size + 1), std::ios::end);
	if (code != -1) {
		std::cout << "driver_fseek errors: -(filesize + 1) offset from END doesn't return -1." << std::endl;
		failed++;
	}

	code = driver_fclose(mf);
	if (code == -1) {
		throw std::runtime_error("driver_fclose error during driver_fseek random tests.");
	}

	return failed;
}

// calls to driver_fseek with offset between 0 and file_size 'times' times
int test_driver_fseek_random(int times = 20) {
	int failed = 0;

	std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";

	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');
	if (mf == nullptr) {
		throw std::runtime_error("driver_fopen error during driver_fseek errors.");
	}
	int code;

	size_t file_size = driver_getFileSize(path.c_str());

	int iter = 0;
	while (iter < times) {
		int rdm = rand() % file_size;

		code = driver_fseek(mf, rdm, std::ios::cur);
		if (code == -1) {
			std::cout << "driver_fseek random test: error seeking from " << mf->pos << " to " << rdm << "." << std::endl;
			failed++;
		}
		iter++;
	}

	code = driver_fclose(mf);
	if (code == -1) {
		throw std::runtime_error("driver_fclose error during driver_fseek random tests.");
	}
	return failed;
}

// crossing the file from begin to end using driver_fseek
int test_driver_fseek_all_file() {
	std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";

	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');
	if (mf == nullptr) {
		throw std::runtime_error("driver_fopen error during driver_fseek all file test.");
	}
	int code;

	size_t total_seek = 0;

	size_t total_seek_target = driver_getFileSize(path.c_str());
	size_t offset = 5678;
	while (total_seek + offset < total_seek_target) {

		code = driver_fseek(mf, offset, std::ios::cur);
		if (code != -1) {
			total_seek += offset;
		}
		else {
			std::cout << "driver_fseek all file test: error seeking the whole file." << std::endl;
			return 1;
		}
	}

	size_t final_offset = total_seek_target - total_seek - 1;
	code = driver_fseek(mf, final_offset, std::ios::cur);
	if (code != -1) {
		total_seek += offset;
	}
	else {
		std::cout << "driver_fseek all file test: error seeking the whole file (last call)." << std::endl;
		std::cout << driver_getlasterror() << std::endl;
		return 1;
	}

	code = driver_fclose(mf);
	if (code == -1) {
		throw std::runtime_error("driver_close error during driver_fseek all file test.");
	}
	return 0;
}

// crossing the file from end to begin using driver_fseek
int test_driver_fseek_all_file_reverse() {
	std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";

	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');
	if (mf == nullptr) {
		throw std::runtime_error("driver_fopen error during driver_fseek all file test.");
	}
	int code;

	size_t total_seek = 0;
	code = driver_fseek(mf, 0, std::ios::end);
	if (code == -1) {
		std::cout << "driver_fseek all file reverse test: error seeking the whole file." << std::endl;
		std::cout << driver_getlasterror() << std::endl;
		return 1;
	}
	//dump_multifile(mf);

	size_t total_seek_target = driver_getFileSize(path.c_str());
	size_t offset_step = 1000;
	while (total_seek + offset_step < total_seek_target) {

		code = driver_fseek(mf, -offset_step, std::ios::cur);
		if (code != -1) {
			total_seek += offset_step;
		}
		else {
			std::cout << "driver_fseek all file reverse test: error seeking the whole file." << std::endl;
			std::cout << driver_getlasterror() << std::endl;
			return 1;
		}
	}

	size_t final_offset = total_seek_target - total_seek;
	code = driver_fseek(mf, -final_offset, std::ios::cur);
	if (code != -1) {
		total_seek += offset_step;
	}
	else {
		std::cout << "driver_fseek all file reverse test: error seeking the whole file (last call)." << std::endl;
		std::cout << driver_getlasterror() << std::endl;
		return 1;
	}
	// dump_multifile(mf);

	code = driver_fclose(mf);
	if (code == -1) {
		throw std::runtime_error("driver_close error during driver_fseek all file test.");
	}
	return 0;
}

void print_file_size_error(const char* path, int exp, int got) {
	std::cout << "test getFileSize error: invalid result for (" << path << "): exp (" << exp << ") | got (" << got << ")" << std::endl;
}

int test_file_size() {
	int failed = 0;

	const char* path = "parquet://C/Users/KXFJ3896/Documents/Parquet-Integration/khiopsdriver_multifile/tests/files/titi.txt";
	int code = driver_getFileSize(path);
	int exp = -1;
	if (code != exp) {
		print_file_size_error(path, exp, code);
		failed++;
	}

	path = "parquet://C/Users/KXFJ3896/Documents/parquet_reader/data/toto.parquet";
	code = driver_getFileSize(path);
	exp = 124;
	if (code != exp) {
		print_file_size_error(path, exp, code);
		failed++;
	}

	path = "askdfjaslk;df";
	code = driver_getFileSize(path);
	exp = -1;
	if (code != exp) {
		print_file_size_error(path, exp, code);
		failed++;
	}

	/*path = "parquet://C/Users/KXFJ3896/Documents/Parquet-Integration/khiopsdriver_multifile/tests/files/empty.";
	code = driver_getFileSize(path);
	exp = 0;
	if (code != exp) {
		print_file_size_error(path, exp, code);
		failed++;
	}*/

	path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";
	code = driver_getFileSize(path);
	exp = 5037442;
	if (code != exp) {
		print_file_size_error(path, exp, code);
		failed++;
	}

	return failed;
}

int main() {
	std::cout << "Driver tests:" << std::endl;

	int failed = 0;

	failed += test_driver_fopen_errors();
	failed += test_driver_fclose_erros();
	failed += test_driver_fread_errors();
	failed += test_driver_fread();
	failed += test_driver_fread_all_file();
	failed += test_driver_fread_whole_file_in_one_read();
	failed += test_driver_fseek_errors();
	failed += test_driver_fseek_random();
	failed += test_driver_fseek_all_file();
	failed += test_driver_fseek_all_file_reverse();

	failed += test_file_size();

	if (failed == 0) {
		std::cout << "PASSED: All tests passed" << std::endl;
	}
	else {
		std::cout << "FAILED: " << failed << " tests failed" << std::endl;
	}
	return 0;
}