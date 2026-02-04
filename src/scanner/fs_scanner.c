/*
 * fs_scanner.c
 *
 * File system scanner for backup directories
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


#include "pg_backup_auditor.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

/*
 * Helper: Add backup to the end of the list
 */
static void
add_backup_to_list(BackupInfo **head, BackupInfo *new_backup)
{
	if (*head == NULL)
	{
		*head = new_backup;
	}
	else
	{
		BackupInfo *current = *head;
		while (current->next != NULL)
			current = current->next;
		current->next = new_backup;
	}
}

/*
 * Helper: Scan a single directory for a backup
 * Returns NULL if no backup detected, or BackupInfo if found
 */
static BackupInfo*
scan_single_directory(const char *path)
{
	BackupAdapter *adapter;
	BackupInfo *backup;

	/* Try to detect backup type using adapters */
	adapter = detect_backup_type(path);
	if (adapter == NULL)
		return NULL;

	log_debug("Detected %s backup at: %s", adapter->name, path);

	/* Use adapter to scan and parse metadata */
	backup = adapter->scan(path);
	if (backup == NULL)
	{
		log_warning("Failed to parse backup metadata at: %s", path);
		return NULL;
	}

	return backup;
}

/*
 * Helper: Recursively scan directory tree for backups
 * max_depth: maximum recursion depth (0 = current dir only, -1 = unlimited)
 */
static void
scan_directory_recursive(const char *dir_path, BackupInfo **backup_list, int depth, int max_depth)
{
	DIR *dir;
	struct dirent *entry;
	struct stat statbuf;
	char path[PATH_MAX];
	BackupInfo *backup;

	/* Check depth limit */
	if (max_depth >= 0 && depth > max_depth)
		return;

	/* Open directory */
	dir = opendir(dir_path);
	if (dir == NULL)
	{
		log_debug("Cannot open directory: %s", dir_path);
		return;
	}

	log_debug("Scanning directory (depth=%d): %s", depth, dir_path);

	/* Try to detect backup in current directory */
	backup = scan_single_directory(dir_path);
	if (backup != NULL)
	{
		add_backup_to_list(backup_list, backup);
	}

	/* Scan subdirectories */
	while ((entry = readdir(dir)) != NULL)
	{
		/* Skip . and .. */
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		/* Build full path */
		snprintf(path, sizeof(path), "%s/%s", dir_path, entry->d_name);

		/* Check if it's a directory */
		if (stat(path, &statbuf) != 0)
		{
			log_warning("Failed to stat: %s", path);
			continue;
		}

		if (!S_ISDIR(statbuf.st_mode))
			continue;

		/* Recurse into subdirectory */
		scan_directory_recursive(path, backup_list, depth + 1, max_depth);
	}

	closedir(dir);
}

/*
 * Scan directory recursively for backups
 * Can detect multiple backup types in the same directory
 *
 * max_depth: maximum recursion depth
 *   -1 = unlimited recursion (scan all subdirectories)
 *    0 = scan only the specified directory
 *    1 = scan directory + 1 level of subdirectories
 *    N = scan up to N levels deep
 */
BackupInfo*
scan_backup_directory(const char *backup_dir, int max_depth)
{
	BackupInfo *backup_list = NULL;

	log_debug("Starting recursive backup scan: %s (max_depth=%d)", backup_dir, max_depth);

	/* Recursively scan with specified max depth */
	scan_directory_recursive(backup_dir, &backup_list, 0, max_depth);

	return backup_list;
}

/*
 * Scan WAL archive directory
 */
WALArchiveInfo*
scan_wal_archive(const char *wal_archive_dir)
{
	/* TODO: Implement WAL archive scanning
	 *
	 * - List all files in directory
	 * - Filter WAL segment files (24 hex chars)
	 * - Parse filenames to extract timeline, log_id, seg_id
	 * - Sort segments
	 * - Return WALArchiveInfo structure
	 */

	(void) wal_archive_dir;  /* unused for now */

	return NULL;
}

/*
 * Free BackupInfo list
 */
void
free_backup_list(BackupInfo *list)
{
	BackupInfo *current = list;
	BackupInfo *next;

	while (current != NULL)
	{
		next = current->next;
		free(current);
		current = next;
	}
}

/*
 * Free WALArchiveInfo
 */
void
free_wal_archive_info(WALArchiveInfo *info)
{
	if (info != NULL)
	{
		if (info->segments != NULL)
			free(info->segments);
		free(info);
	}
}
