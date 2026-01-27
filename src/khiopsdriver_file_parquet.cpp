// Copyright (c) 2023-2025 Orange. All rights reserved.
// This software is distributed under the BSD 3-Clause-clear License, the text of which is available
// at https://spdx.org/licenses/BSD-3-Clause-Clear.html or see the "LICENSE" file for more details.

// Pour eviter les warning sur strerror
#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "khiopsdriver_file_parquet.h"
#include "parquet_file.h"

#if defined(__linux__) || defined(__APPLE__)
#define __linux_or_apple__
#endif

#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <errno.h>
#include <sys/stat.h>

#ifdef _MSC_VER
#include <direct.h>
#include <io.h>
#include <windows.h>
#endif // _MSC_VER

#ifdef __linux_or_apple__
#include <unistd.h>
#ifdef __gnu_linux__
#include <sys/vfs.h> // ANDROID https://svn.boost.org/trac/boost/ticket/8816
#else
#include <sys/statvfs.h>
#endif // __clang__
#endif // __linux_or_apple__

// Define to compile a read-only version of the driver
// Uncomment the following line to compile the read-only version of the driver
#define __nullreadonlydriver__

using parquet::TypedColumnReader;
using parquet::Type;

static thread_local const char* g_lastError;

void LogError(const char* msg) {
	g_lastError = std::move(msg);
}

const char* driver_getDriverName()
{
	return "Parquet driver";
}

const char* driver_getVersion()
{
	return "0.0.1";
}

const char* driver_getScheme()
{
	return "parquet";
}

int driver_isReadOnly()
{
	return 1;
}

int driver_connect()
{
	return 0;
}

int driver_disconnect()
{
	return 1;
}

int driver_isConnected()
{
	return 1;
}

// Nombre de caracteres du nom du scheme
int getSchemeCharNumber()
{
	static const int nSchemeCharNumber = (int)strlen(driver_getScheme());
	return nSchemeCharNumber;
}

// Test si un fichier est gere par le scheme
int isManaged(const char* sFilePathName)
{
	int ok;

	assert(sFilePathName != NULL);

	// Le debut du nom de fichier doit etre de la forme 'scheme://' ou 'scheme:///'
	ok = strncmp(sFilePathName, driver_getScheme(), getSchemeCharNumber()) == 0;
	ok = ok && sFilePathName[getSchemeCharNumber()] == ':';
	ok = ok && sFilePathName[getSchemeCharNumber() + 1] == '/';
	ok = ok && sFilePathName[getSchemeCharNumber() + 2] == '/';
	return ok;
}

// Methode utilitaire pour avoir acces au nom du fichier sans son schema
const char* getFilePath(const char* sFilePathName)
{
	int nStartFilePath;

	// La gestion du nombre de '/' n'est pas claire
	// Selon https://en.wikipedia.org/wiki/File_URI_scheme , on peut avoir de 1 a 4 '/' selon les cas
	// Des tests sous windows avec un navigateur firefox ou chrome montrent un grande tolerance au nombre de '/'.
	// Pourvue que le nom commence par 'file:', on peut avoir un nombre quelconque de '/', meme zero.
	// Firefox le corrige en mettant 'file:///'<path>, et chrone en omettant le scheme et en gardant juste <path>
	// On decide ici d'appliquer une politique souple, avec un nombre quelconque de '/', au moins un sous linux.

	// On extrait le chemin du fichier si le schema est correct
	if (isManaged(sFilePathName))
	{
		// Le debut du nom de fichier doit etre de la forme 'scheme:' suivi d'un nombre quelconque de '/'
		// Sous windows, on se place on premier caractere non '/', et sous linux, on inclus le dernier '/'
		// On renvoie un path commencant par '/'
		nStartFilePath = getSchemeCharNumber() + 1;
		while (sFilePathName[nStartFilePath] == '/')
			nStartFilePath++;
		assert(sFilePathName[nStartFilePath] != '/');
#ifndef _MSC_VER
		nStartFilePath--;
		assert(sFilePathName[nStartFilePath] == '/');
#endif // _MSC_VER
		return &sFilePathName[nStartFilePath];
	}
	// Sinon, on renvoie le nom du fichier tel quel
	else
		return sFilePathName;
}

int driver_fileExists(const char* filename)
{
	int bIsFile = false;

	// Temporary solution because Khiops accept only one ':' 
	// so impossible because this driver need the scheme (parquet://...)
	// turning path from: C/path/to/file.parquet
	// into: C:/path/to/file.parquet

	const char* file_path = getFilePath(filename);

	char* valid_path = (char*)malloc((strlen(file_path) + 2) * sizeof(char));
	if (valid_path == NULL) {
		LogError("driver_fopen: Unable to malloc to add \':\' to path.");
		return NULL;
	}

	valid_path[0] = file_path[0];
	valid_path[1] = ':';
	for (size_t i = 1; i <= strlen(file_path); i++)
		valid_path[i + 1] = file_path[i];

	valid_path[strlen(file_path) + 1] = '\0';
	// end of temporary solution

#ifdef _WIN32
	struct __stat64 fileStat;
	if (_stat64(valid_path, &fileStat) == 0)
		bIsFile = ((fileStat.st_mode & S_IFMT) == S_IFREG);
#else
	struct stat s;
	if (stat(valid_path, &s) == 0)
		bIsFile = ((s.st_mode & S_IFMT) == S_IFREG);
#endif // _WIN32

	return bIsFile;
}

int driver_dirExists(const char* filename)
{
	int bIsDirectory = false;

#ifdef _WIN32
	boolean bExist;

	bExist = _access(getFilePath(filename), 0) != -1;
	if (bExist)
	{
		// On test si ca n'est pas un fichier, car sous Windows, la racine ("C:") existe mais n'est
		// consideree par l'API _stat64 ni comme une fichier ni comme un repertoire
		boolean bIsFile = false;
		struct __stat64 fileStat;
		if (_stat64(filename, &fileStat) == 0)
			bIsFile = ((fileStat.st_mode & S_IFMT) == S_IFREG);
		bIsDirectory = !bIsFile;
	}
#else // _WIN32

	struct stat s;
	if (stat(getFilePath(filename), &s) == 0)
		bIsDirectory = ((s.st_mode & S_IFMT) == S_IFDIR);

#endif // _WIN32

	return bIsDirectory;
}

long long int driver_getFileSize(const char* filename)
{
	if (filename == nullptr) {
		return -1;
	}

	// Temporary solution because Khiops accept only one ':' 
	// so impossible because this driver need the scheme (parquet://...)
	// turning path from: C/path/to/file.parquet
	// into: C:/path/to/file.parquet

	const char* file_path = getFilePath(filename);

	char* valid_path = (char*)malloc((strlen(file_path) + 2) * sizeof(char));
	if (valid_path == NULL) {
		LogError("driver_getFileSize: Unable to malloc to add \':\' to path.");
		return -1;
	}

	// std::cout << "driver_getFileSize called: filename=" << filename << std::endl;

	valid_path[0] = file_path[0];
	valid_path[1] = ':';
	for (size_t i = 1; i <= strlen(file_path); i++)
		valid_path[i + 1] = file_path[i];

	valid_path[strlen(file_path) + 1] = '\0';
	// end of temporary solution
	try {
		ParquetFile parquetFile = ParquetFile(valid_path);
		return parquetFile.logical_size;
	}
	catch (const std::exception& e) {
		LogError("driver_getFileSize: Unable to open parquet file to get its size.");
		return -1;
	}
}

void* driver_fopen(const char* filename, char mode)
{
	void* handle;

	if (mode != 'r' || filename == nullptr) {
		LogError("driver_fopen: Invalid mode or NULL filename.");
		return nullptr;
	}
	// std::cout << "driver_fopen called: filename=" << filename << ", mode=" << mode << std::endl;

	// Temporary solution because Khiops accept only one ':' 
	// so impossible because this driver need the scheme (parquet://...)
	// turning path from: C/path/to/file.parquet
	// into: C:/path/to/file.parquet

	const char* file_path = getFilePath(filename);

	char* valid_path = (char*)malloc((strlen(file_path) + 2) * sizeof(char));
	if (valid_path == NULL) {
		LogError("driver_fopen: Unable to malloc to add \':\' to path.");
		return NULL;
	}

	valid_path[0] = file_path[0];
	valid_path[1] = ':';
	for (size_t i = 1; i <= strlen(file_path); i++)
		valid_path[i + 1] = file_path[i];

	valid_path[strlen(file_path) + 1] = '\0';
	// end of temporary solution

	try {
		handle = new ParquetFile(valid_path);
	}
	catch (...) {
		LogError("driver_fopen: Unable to open parquet file.");
		return nullptr;
	}
	
	return handle;
}

int driver_fclose(void* stream)
{
	//std::cout << "driver_fclose called." << std::endl;
	if (!stream) {
		LogError("driver_fclose: NULL ParquetFile pointer.");
		return EOF;
	}

	ParquetFile* pf = static_cast<ParquetFile*>(stream);

	if (!pf->isOpen()) {
		LogError("driver_fclose: ParquetFile already closed.");
		return EOF;
	}

	pf->close();
	delete pf;
	return 0;
}


long long int driver_fread(void* ptr, size_t size, size_t count, void* stream)
{
	if (!ptr || !stream) {
		LogError("driver_fread: NULL pointer argument.");
		return -1;
	}
	// std::cout << "driver_fread called: size=" << size << ", count=" << count << std::endl;

	ParquetFile* parquetFile = static_cast<ParquetFile*>(stream);
	if (!parquetFile->isOpen()) {
		LogError("driver_fread: ParquetFile is not open.");
		return -1;
	}
	uint8_t* out = static_cast<uint8_t*>(ptr);  // important !
	size_t totalBytesToRead = size * count;
	size_t readcount = 0;

	// finding the row group or header at current position

	size_t header = -1;
	size_t rg_id = parquetFile->find_row_group(header);
	if (rg_id == -1 && header == -1) {
		return 0;
	}
	else if (header != -1) {
		while (header < parquetFile->headers.size() && readcount < totalBytesToRead) { // reading header loop
			auto value_logical_start = parquetFile->headers[header].header_logical_start;
			size_t offset_in_value = parquetFile->pos - value_logical_start;

			std::string value;
			if (!parquetFile->readHeader(header, value)) return -1;

			size_t valueSize = value.size();

			size_t nb_to_copy = min(valueSize - offset_in_value, totalBytesToRead - readcount);

			std::memcpy(out + readcount, value.data() + offset_in_value, nb_to_copy);
			readcount += nb_to_copy;

			parquetFile->pos += nb_to_copy;

			if (parquetFile->pos > parquetFile->headers[header].header_logical_end) {
				header++;
			}
		}

		rg_id = 0;
	}

	// reading row group
	std::vector<std::shared_ptr<arrow::RecordBatch>> batches = parquetFile->read(rg_id);

	size_t col = 0;
	size_t row = 0;
	size_t batch_id = 0;
	
	// find where we are in the row group
	auto logical_start = parquetFile->row_groups[rg_id].rowgroup_logical_start;
	int64_t offset_in_rg = parquetFile->pos - logical_start;
	int64_t cur_offset = 0;


	std::string value;
	while (cur_offset < offset_in_rg) {
		std::shared_ptr<arrow::RecordBatch> batch = batches[batch_id];
		std::shared_ptr<arrow::Array> array = batch->column(col);
		
		value = array->GetScalar(row)->get()->ToString();

		bool need_quote = false;
		for (size_t i = 0; i < value.size(); i++) {
			if (value[i] == parquetFile->sep || value[i] == '"' || value[i] == '\n') {
				need_quote = true;
				break;
			}
		}

		std::string escaped;
		if (!need_quote) {
			escaped = value;
		}
		else {
			escaped = '"';
			for (size_t i = 0; i < value.size(); i++) {
				if (value[i] == '"') {
					escaped.push_back('"');
				}
				escaped.push_back(value[i]);
			}
			escaped.push_back('"');
		}


		if (col == batch->num_columns() - 1) {
			escaped.push_back('\n');
		}
		else
		{
			escaped.push_back(parquetFile->sep);
		}


		if (cur_offset + escaped.size() >= offset_in_rg) {
			size_t offset_in_value = offset_in_rg - cur_offset;
			int64_t to_read_from_value = escaped.size() - offset_in_value;
			int64_t remaining_bytes = totalBytesToRead - readcount;

			int64_t nb_to_copy = min(to_read_from_value, remaining_bytes);

			std::memcpy(out + readcount, escaped.data() + offset_in_value, nb_to_copy);

			readcount += nb_to_copy;
			parquetFile->pos += nb_to_copy;
		}
		cur_offset += escaped.size();	// Ok meme si toute valeur non lu car 
										// si la valeur est pas lu entierement cela signifie qu'on a fini de lire
		
		col++;
		if (col == batch->num_columns()) {
			row++;
			col = 0;
		}
		if (row == batch->num_rows()) {
			batch_id++;
			row = 0;
		}
	}

	while (readcount < totalBytesToRead && parquetFile->pos < parquetFile->logical_size)
	{
		std::shared_ptr<arrow::RecordBatch> batch = batches[batch_id];
		std::shared_ptr<arrow::Array> array = batch->column(col);

		value = array->GetScalar(row)->get()->ToString();

		bool need_quote = false;
		for (size_t i = 0; i < value.size(); i++) {
			if (value[i] == parquetFile->sep || value[i] == '"' || value[i] == '\n') {
				need_quote = true;
				break;
			}
		}

		std::string escaped;
		if (!need_quote) {
			escaped = value;
		}
		else {
			escaped = '"';
			for (size_t i = 0; i < value.size(); i++) {
				if (value[i] == '"') {
					escaped.push_back('"');
				}
				escaped.push_back(value[i]);
			}
			escaped.push_back('"');
		}

		if (col == batch->num_columns() - 1) {
			escaped.push_back('\n');
		}
		else
		{
			escaped.push_back(parquetFile->sep);
		}

		size_t valueSize = escaped.size();

		int64_t remaining_bytes = totalBytesToRead - readcount;

		int64_t nb_to_copy = min(valueSize, remaining_bytes);

		std::memcpy(out + readcount, escaped.data(), nb_to_copy);
		
		readcount += nb_to_copy;

		parquetFile->pos += nb_to_copy;

		col++;
		if (col >= batch->num_columns()) {
			row++;
			col = 0;
		}
		if (row >= batch->num_rows()) {
			batch_id++;
			row = 0;
		}
		if (batch_id >= batches.size()) {
			// finished this row group
			rg_id++;
			if (rg_id >= parquetFile->row_groups.size()) {
				// finished all row groups
				break;
			}
			batches = parquetFile->read(rg_id);
			batch_id = 0;
			row = 0;
			col = 0;
		}
	}

	return readcount;
}

int driver_fseek(void* stream, long long int offset, int whence)
{
	if (stream == nullptr) {
		LogError("driver_fseek: NULL ParquetFile pointer.");
		return -1;
	}
	// std::cout << "driver_fseek called: offset=" << offset << ", whence=" << whence << std::endl;

	ParquetFile* parquetFile = static_cast<ParquetFile*>(stream);
	if (!parquetFile->isOpen()) {
		LogError("driver_fread: ParquetFile is not open.");
		return -1;
	}
	if (parquetFile == NULL) return -1; // possiblement inutile
		
	uint64_t newPos = -1;
	if (whence == std::ios::beg) {
		if (offset >= 0 && offset <= (long long int)parquetFile->logical_size) {
			newPos = offset;
		}
	}
	else if (whence == std::ios::cur) {
		if (parquetFile->pos + offset >= 0 && parquetFile->pos + offset <= (long long int)parquetFile->logical_size) {
			newPos = parquetFile->pos + offset;
		}
	}
	else if (whence == std::ios::end) {
		if (parquetFile->logical_size + offset >= 0 && parquetFile->logical_size + offset <= (long long int)parquetFile->logical_size) {
			newPos = parquetFile->logical_size + offset;
		}
	}
	else {
		LogError("diver_fseek: Invalid whence.");
		return -1;
	}

	if (newPos == (uint64_t)-1) {
		LogError("driver_fseek: Invalid offset value (out of bounds).");
		return -1;
	}

	
	parquetFile->pos = newPos;
	return 0;
}

const char* driver_getlasterror()
{
	return g_lastError;
}
