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

#ifdef _WIN32
	struct __stat64 fileStat;
	if (_stat64(getFilePath(filename), &fileStat) == 0)
		bIsFile = ((fileStat.st_mode & S_IFMT) == S_IFREG);
#else
	struct stat s;
	if (stat(getFilePath(filename), &s) == 0)
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
	int code = EOF;
	if (stream == nullptr) {
		LogError("driver_fclose: NULL ParquetFile pointer.");
		return code;
	}

	ParquetFile* parquetFile = static_cast<ParquetFile*>(stream);
	if (parquetFile != NULL) {
		code = 0;
		delete (ParquetFile*)stream;
	}

	return code;
}

long long int driver_fread(void* ptr, size_t size, size_t count, void* stream)
{
	if (!ptr || !stream) {
		LogError("driver_fread: NULL pointer argument.");
		return -1;
	}

	ParquetFile* parquetFile = static_cast<ParquetFile*>(stream);
	uint8_t* out = static_cast<uint8_t*>(ptr);  // important !
	size_t totalBytesToRead = size * count;
	size_t readcount = 0;

	size_t rg, col, page, val;
	if (!parquetFile->findValueAtLogicalPosition(rg, col, page, val)) { 
		return 0; 
	}

	while (readcount < totalBytesToRead)
	{
		// reading value at offset
		std::vector<uint8_t> valueBytes;

		auto value_logical_start = parquetFile->row_groups[rg].columns[col].pages[page].values[val].value_logical_start;
		size_t offset_in_value = parquetFile->pos - value_logical_start;

		parquetFile->readValue(rg, col, page, val, valueBytes);
		size_t valueSize = valueBytes.size();

		size_t nb_to_copy = min(valueSize-offset_in_value, totalBytesToRead - readcount);

		std::memcpy(out + readcount, valueBytes.data()+offset_in_value, nb_to_copy);
		readcount += nb_to_copy;

		parquetFile->pos += nb_to_copy;

		const RowGroupIndex& rg_idx = parquetFile->row_groups[rg];
		const ColumnIndex& col_idx = rg_idx.columns[col];
		const PageIndex& page_idx = col_idx.pages[page];

		col++;

		if (col >= rg_idx.columns.size()) {
			col = 0;
			val++;
		}

		if (val >= page_idx.values.size()) {
			val = 0;
			page++;
		}

		if (page >= col_idx.pages.size()) {
			page = 0;
			rg++;
		}



		// Fin fichier
		if (rg >= (int)parquetFile->row_groups.size()) {
			break;
		}
	}

	return readcount;
}

int driver_fseek(void* stream, long long int offset, int whence)
{
	int ok = 0;
	if (stream == nullptr) {
		LogError("driver_fseek: NULL ParquetFile pointer.");
		return -1;
	}

	ParquetFile* parquetFile = static_cast<ParquetFile*>(stream);
	if (parquetFile == NULL) return -1; // possiblement inutile
		

	if (whence == std::ios::beg) {
		if (offset >= 0 && offset <= (long long int)parquetFile->logical_size) {
			parquetFile->pos = offset;
			return 0;
		}
	}
	else if (whence == std::ios::cur) {
		if (parquetFile->pos + offset >= 0 && parquetFile->pos + offset <= (long long int)parquetFile->logical_size) {
			parquetFile->pos += offset;
			return 0;
		}
	}
	else if (whence == std::ios::end) {
		if (parquetFile->logical_size + offset >= 0 && parquetFile->logical_size + offset <= (long long int)parquetFile->logical_size) {
			parquetFile->pos = parquetFile->logical_size + offset;
			return 0;
		}
	}
	else {
		LogError("diver_fseek: Invalid whence.");
		return -1;
	}

	LogError("driver_fseek: Unable to seek to the specified offset.");
	return -1;
}

const char* driver_getlasterror()
{
	return g_lastError;
}
