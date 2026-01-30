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

int test_driver_use_after_close() {
	std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";

	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');

	driver_fclose(mf);

	char buf[10];
	if (driver_fread(buf, 1, 10, mf) != -1)
		return 1;

	if (driver_fseek(mf, 0, std::ios::beg) != -1)
		return 1;

	return 0;
}

int test_driver_double_fclose() {
	std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";

	auto* f = driver_fopen(path.c_str(), 'r');
	driver_fclose(f);
	if (driver_fclose(f) != EOF) return 1;
	return 0;
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
	if (!buffer) {
		driver_fclose(mf);
		throw std::runtime_error("driver_fread all file test error: unable to malloc buffer.");
	}

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
		free(buffer);
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
	if (!buffer) {
		driver_fclose(mf);
		throw std::runtime_error("driver_fread all file test error: unable to malloc buffer.");
	}

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
		free(buffer);
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
			free(buffer);
			return 1;
		}
		// std::cout << "END of loop" << std::endl;
	}

	if (total_read != total_read_target) {
		std::cout << "driver_fread all file test: read more than there is in file." << std::endl;
		free(buffer);
		return 1;
	}

	free(buffer);

	code = driver_fclose(mf);
	if (code == -1) {
		throw std::runtime_error("driver_close error during driver_fread all file test.");
	}

	return 0;
}

// read all file from begin to end using driver_fread, fseek to begin and read again
int test_driver_fread_all_file_two_times() {
	std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";

	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');
	if (mf == nullptr) {
		throw std::runtime_error("driver_fopen error during driver_fread all file test.");
	}
	int code;

	const size_t buffer_size = 1000;
	char* buffer = (char*)calloc(buffer_size, sizeof(char));
	if (!buffer) {
		driver_fclose(mf);
		throw std::runtime_error("driver_fread all file test error: unable to malloc buffer.");
	}

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
			free(buffer);
			return 1;
		}
		// std::cout << "END of loop" << std::endl;
	}

	if (total_read != total_read_target) {
		std::cout << "driver_fread all file test: read more than there is in file." << std::endl;
		free(buffer);
		return 1;
	}

	code = driver_fseek(mf, 0, std::ios::beg);
	if (code == -1) {
		free(buffer);
		throw std::runtime_error("driver_fseek error during driver_fread all file test.");
	}

	total_read = 0;
	while (total_read < total_read_target) {
		code = driver_fread(buffer, sizeof(char), read_size_in_loop, mf);

		if (code != -1) {
			total_read += code;
		}
		else {
			std::cout << "driver_fread all file test: error reading the whole file." << std::endl;
			free(buffer);
			return 1;
		}
	}

	if (total_read != total_read_target) {
		std::cout << "driver_fread all file test: read more than there is in file." << std::endl;
		free(buffer);
		return 1;
	}

	code = driver_fclose(mf);
	if (code == -1) {
		free(buffer);
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
	if (buffer == NULL) {
		driver_fclose(mf);
		throw std::runtime_error("driver_fread tests error: unable to malloc buffer.");
	}

	size_t size = 1;
	size_t count = 500;

	code = driver_fread(buffer, size, count, mf);
	if (code == -1) {
		std::cout << "driver_fread tests error: unable to read" << std::endl;
		free(buffer);
		return 1;
	}

	const char* exp = "id\tcountry\tcity\n1\tUSA\tNew York\n2\tCanada\tToronto\n3\tUK\tLondon\n4\tAustralia\tSydney\n5\tGermany\tBerlin\n6\tFrance\tParis\n7\tJapan\tTokyo\n8\tSpain\tMadrid\n";
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

int test_driver_fread_partial_reads() {
	std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";
	
    ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');

    char buf[7];
    int r1 = driver_fread(buf, 1, 3, mf);
    int r2 = driver_fread(buf, 1, 3, mf);
    int r3 = driver_fread(buf, 1, 3, mf);

    if (r1 != 3 || r2 != 3 || r3 != 3)
        return 1;

    driver_fclose(mf);
    return 0;
}


int test_driver_fread_after_eof() {
	std::string path = "parquet://C/Users/KXFJ3896/Documents/parquet_reader/data/toto.parquet";

	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');

	char buf[300];
	size_t size = driver_getFileSize(path.c_str());

	driver_fseek(mf, size, std::ios::beg);

	int r = driver_fread(buf, 1, 300, mf);
	if (r != 0) {
		std::cout << "test fread after eof: reading after eof didn't return 0." << std::endl;
		return 1;
	}

	// appel REFAIT
	r = driver_fread(buf, 1, 300, mf);
	if (r != 0) {
		std::cout << "test fread after eof: reading after eof didn't return 0." << std::endl;
		return 1;
	}

	driver_fclose(mf);
	return 0;
}

int test_driver_fread_fseek_mix() {
	std::string path = "parquet://C/Users/KXFJ3896/Documents/parquet_reader/data/toto.parquet";

	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');

	char buf[32];
	char buf1[32];
	char buf2[32];

	int code;
	int code1;
	int code2;

	code = driver_fread(buf, 1, 30, mf);
	buf[31] = 0;
	driver_fseek(mf, 0, std::ios::beg);

	code1 = driver_fread(buf1, 1, 30, mf);
	buf1[31] = 0;

	if (code != code1) {
		std::cout << "test fread fseek mix: reading two times the beginning of file didn't read same size." << std::endl;
		return 1;
	}
	if (memcmp(buf, buf1, 32) != 0) {
		std::cout << "test fread fseek mix: reading two times the beginning of file didn't return same buffers." << std::endl;
		std::cout << "Buf: " << buf << std::endl;
		std::cout << "Buf1: " << buf1 << std::endl;
		return 1;
	}



	if (driver_fseek(mf, 18, std::ios::cur) == -1) {
		std::cout << "test fread fseek mix: Couldn't fseek 5 from cur." << std::endl;
		return 1;
	}
	if (driver_fread(buf, 1, 32, mf) != 32) {
		std::cout << "test fread fseek mix: Couldn't read 32." << std::endl;
		return 1;
	}

	driver_fclose(mf);
	return 0;
}

int test_driver_fseek_unaligned() {
	std::string path = "parquet://C/Users/KXFJ3896/Documents/parquet_reader/data/toto.parquet";

	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');

	driver_fseek(mf, 1, std::ios::beg);
	char c;
	driver_fread(&c, 1, 1, mf);

	driver_fseek(mf, 2, std::ios::beg);
	driver_fread(&c, 1, 1, mf);

	driver_fclose(mf);
	return 0;
}

int test_driver_fread_byte_by_byte() {
	std::string path = "parquet://C/Users/KXFJ3896/Documents/parquet_reader/data/toto.parquet";

	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');

	char c;
	size_t total = 0;
	while (driver_fread(&c, 1, 1, mf) == 1) {
		total++;
	}

	if (total != driver_getFileSize(path.c_str()))
		return 1;

	driver_fclose(mf);
	return 0;
}

int test_driver_fread_eof() {
	std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";


	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');

	int code;
	code = driver_fseek(mf, 0, std::ios::end);
	if (code == -1) {
		std::cout << "driver_fread eof test: error seeking to end of file." << std::endl;
		return 1;
	}

	char buf[10];
	code = driver_fread(buf, 1, 10, mf);
	if (code != 0) {
		std::cout << "driver_fread eof test: reading at end of file didn't return 0." << std::endl;
		return 1;
	}

	code = driver_fclose(mf);
	if (code == -1) {
		std::cout << "driver_fclose error during driver_fread eof test." << std::endl;
		return 1;
	}

	return 0;
}

int test_driver_fread_multiple_time() {
	std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";

	size_t total_size = driver_getFileSize(path.c_str());

	ParquetFile* mf = (ParquetFile*)driver_fopen(path.c_str(), 'r');

	int code;
	code = driver_fseek(mf, 1048576, std::ios::beg);
	if (code == -1) {
		std::cout << "driver_fread eof test: error seeking to end of file." << std::endl;
		return 1;
	}

	char* buf = (char*)calloc(1048576+1,sizeof(char));
	if (!buf) {
		driver_fclose(mf);
		throw std::runtime_error("driver_fread_multiple_time test error: unable to malloc buffer.");
	}

	code = driver_fread(buf, 1, 1048576, mf);
	if (code == -1) {
		std::cout << "driver_fread multiple time test: error reading after seeking to 1MB from BEGIN." << std::endl;
		free(buf);
		return 1;
	}

	for (size_t i = 0; i < 100; i++){
		size_t random_seek = rand() % (1048576 + 1);

		code = driver_fseek(mf, random_seek, std::ios::beg);

		if (code == -1) {
			std::cout << "driver_fread multiple time test: error seeking to " << random_seek << " from BEGIN." << std::endl;
			free(buf);
			return 1;
		}

		//size_t random_read = rand() % (1048576 + 1);
		size_t random_read = 1048576;

		code = driver_fread(buf, 1, random_read, mf);
		if (code == -1) {
			std::cout << "driver_fread multiple time test: error reading after seeking to " << random_read << " from BEGIN." << std::endl;
			free(buf);
			return 1;
		}
	}

	code = driver_fclose(mf);
	if (code == -1) {
		std::cout << "driver_fclose error during driver_fread multiple time test." << std::endl;
		free(buf);
		return 1;
	}

	free(buf);
	return 0;
}

int read_after_fseek() {
	const std::string path = "parquet://C/Users/KXFJ3896/Documents/parquet_reader/data/test.parquet";

	size_t file_logical_size = driver_getFileSize(path.c_str());
	//std::cout << "File logical size: " << file_logical_size << std::endl;

	void* driver = driver_fopen(path.c_str(), 'r');
	if (!driver) {
		std::cerr << "read_after_fseek error: driver_fopen failed" << std::endl;
		return 1;
	}
	auto pd = (ParquetFile*)driver;

	size_t buffer_size = 30;
	void* buf = calloc(1, buffer_size + 1);
	if (!buf) {
		std::cerr << "read_after_fseek error: Unable to calloc\n";
		return 1;
	}

	std::string exp_str;
	int seek_position = 16;
	long long code = driver_fseek(driver, seek_position, 0);

	if (code == -1) {
		std::cerr << "read_after_fseek error: driver_fseek returned -1 on seeking to " << seek_position << std::endl;
		free(buf);
		return 1;
	}


	code = driver_fread(buf, 1, 19, driver);
	long long exp = 19;
	if (code != -1) {
		exp_str = "to\t1\n1\t\"it\"\"it\"\t42\n";
		if (strncmp((char*)buf, exp_str.c_str(),exp) != 0 || code != exp) {
			std::cout << "read_after_fseek error: content of buffer != expected or number of bytes read != exp." << std::endl;

			std::cout << "Read bytes = " << code << std::endl;

			((char*)buf)[code] = 0;
			std::cout << "Buffer contains: " << std::endl << (char*)buf << "<-EOF" << std::endl;

			std::cout << std::endl;
			free(buf);
			return 1;
		}

	}
	else {
		std::cerr << "read_after_fseek error: driver_fread returned -1\n";
		free(buf);
		return 1;
	}

	seek_position = 59;

	code = driver_fseek(driver, seek_position, 0);

	if (code == -1) {
		std::cerr << "read_after_fseek error: driver_fseek returned -1 on seeking to " << seek_position << std::endl;
		free(buf);
		return 1;
	}


	code = driver_fread(buf, 1, 19, driver);
	exp = 13;
	if (code != -1) {
		exp_str = "123456789\t\t0\n";
		if (strncmp((char*)buf, exp_str.c_str(),exp) != 0 || code != exp) {
			std::cout << "read_after_fseek error: content of buffer != expected or number of bytes read != exp." << std::endl;

			std::cout << "Read bytes = " << code << std::endl;

			((char*)buf)[code] = 0;
			std::cout << "Buffer contains: " << std::endl << (char*)buf << "<-EOF" << std::endl;

			std::cout << std::endl;

			free(buf);
			return 1;
		}

	}
	else {
		std::cerr << "read_after_fseek error: driver_fread returned -1\n";
		free(buf);
		return 1;
	}

	free(buf);

	if (driver_fclose(driver) != 0) {
		std::cerr << "Error closing driver\n";
	}
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
		std::cout << "driver_fseek all file reverse test: error seeking to the end of the file." << std::endl;
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
		throw std::runtime_error("driver_close error during driver_fseek all file reverse test.");
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
	exp = 140;
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
	exp = 5037445;
	if (code != exp) {
		print_file_size_error(path, exp, code);
		failed++;
	}

	return failed;
}

int test_driver_fileExists() {
	int failed = 0;

	std::string path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/Places.parquet";
	bool exist = driver_fileExists(path.c_str());
	if (!exist) {
		std::cout << "driver_fileExists test error: existing file not found." << std::endl;
		failed++;
	}

	path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/NonExistent.parquet";
	exist = driver_fileExists(path.c_str());
	if (exist) {
		std::cout << "driver_fileExists test error: non-existing file found." << std::endl;
		failed++;
	}

	path = "parquet://C/Users/Public/khiops_data/samples/AccidentsMedium/";
	exist = driver_fileExists(path.c_str());
	if (exist) {
		std::cout << "driver_fileExists test error: directory found as file." << std::endl;
		failed++;
	}

	path = "C/Users/KXFJ3896/Documents/parquet_reader/data/toto.parquet";
	exist = driver_fileExists(path.c_str());
	if (!exist) {
		std::cout << "driver_fileExists test error: existing file not found." << std::endl;
		failed++;
	}
	return failed;
}

int main() {
	std::cout << "Driver tests:" << std::endl;

	int failed = 0;

	failed += test_driver_fopen_errors();
	failed += test_driver_fclose_erros();
	failed += test_driver_use_after_close();
	failed += test_driver_double_fclose();

	failed += test_driver_fread_errors();
	failed += test_driver_fread();
	failed += test_driver_fread_all_file();
	failed += test_driver_fread_whole_file_in_one_read();
	failed += test_driver_fread_all_file_two_times();
	failed += test_driver_fread_partial_reads();
	failed += test_driver_fread_after_eof();
	failed += test_driver_fread_fseek_mix();
	failed += test_driver_fread_byte_by_byte();
	failed += test_driver_fread_eof();
	failed += test_driver_fread_multiple_time();
	failed += read_after_fseek();

	failed += test_driver_fseek_errors();
	failed += test_driver_fseek_random();
	failed += test_driver_fseek_all_file();
	failed += test_driver_fseek_all_file_reverse();
	failed += test_driver_fseek_unaligned();

	failed += test_file_size();
	failed += test_driver_fileExists();

	if (failed == 0) {
		std::cout << "PASSED: All tests passed" << std::endl;
	}
	else {
		std::cout << "FAILED: " << failed << " tests failed" << std::endl;
	}
	return 0;
}