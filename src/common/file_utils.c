/*
 * file_utils.c
 *
 * File system utilities
 *
 * Copyright (C) 2026  Daria
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#define _POSIX_C_SOURCE 200809L

#include "pg_backup_auditor.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

/*
 * Check if path exists
 */
bool
file_exists(const char *path)
{
	struct stat st;
	return (stat(path, &st) == 0);
}

/*
 * Check if path is a directory
 */
bool
is_directory(const char *path)
{
	struct stat st;

	if (stat(path, &st) != 0)
		return false;

	return S_ISDIR(st.st_mode);
}

/*
 * Check if path is a regular file
 */
bool
is_regular_file(const char *path)
{
	struct stat st;

	if (stat(path, &st) != 0)
		return false;

	return S_ISREG(st.st_mode);
}

/*
 * Get file size
 */
off_t
get_file_size(const char *path)
{
	struct stat st;

	if (stat(path, &st) != 0)
		return -1;

	return st.st_size;
}

/*
 * Get total size of directory (recursively)
 * Returns total size in bytes
 */
uint64_t
get_directory_size(const char *path)
{
	DIR *dir;
	struct dirent *entry;
	struct stat st;
	uint64_t total_size = 0;
	char full_path[PATH_MAX];

	dir = opendir(path);
	if (dir == NULL)
		return 0;

	while ((entry = readdir(dir)) != NULL)
	{
		/* Skip . and .. */
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		/* Build full path */
		path_join(full_path, sizeof(full_path), path, entry->d_name);

		if (stat(full_path, &st) != 0)
			continue;

		if (S_ISDIR(st.st_mode))
		{
			/* Recursively get subdirectory size */
			total_size += get_directory_size(full_path);
		}
		else if (S_ISREG(st.st_mode))
		{
			/* Add file size */
			total_size += st.st_size;
		}
	}

	closedir(dir);
	return total_size;
}

/*
 * Join paths (similar to Python's os.path.join)
 */
void
path_join(char *dest, size_t destsize, const char *path1, const char *path2)
{
	size_t len1 = strlen(path1);

	/* Copy first path */
	str_copy(dest, path1, destsize);

	/* Add separator if needed */
	if (len1 > 0 && dest[len1 - 1] != '/')
	{
		if (len1 + 1 < destsize)
		{
			dest[len1] = '/';
			dest[len1 + 1] = '\0';
			len1++;
		}
	}

	/* Skip leading slash in path2 */
	if (*path2 == '/')
		path2++;

	/* Append second path */
	if (len1 < destsize)
		str_copy(dest + len1, path2, destsize - len1);
}

/*
 * Read entire file into string
 * Caller must free the returned string
 */
char *
read_file_contents(const char *path)
{
	FILE *f;
	long size;
	char *contents;
	size_t read_size;

	f = fopen(path, "r");
	if (f == NULL)
		return NULL;

	/* Get file size */
	fseek(f, 0, SEEK_END);
	size = ftell(f);
	fseek(f, 0, SEEK_SET);

	if (size < 0)
	{
		fclose(f);
		return NULL;
	}

	/* Allocate buffer */
	contents = malloc(size + 1);
	if (contents == NULL)
	{
		fclose(f);
		return NULL;
	}

	/* Read file */
	read_size = fread(contents, 1, size, f);
	contents[read_size] = '\0';

	fclose(f);

	return contents;
}
