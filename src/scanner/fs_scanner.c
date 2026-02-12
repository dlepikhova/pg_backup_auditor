/*
 * fs_scanner.c
 *
 * File system scanner for backup directories
 *
 * Copyright (C) 2026 Daria Lepikhova
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
 * Compare function for qsort - sort WAL segments
 */
static int
compare_wal_segments(const void *a, const void *b)
{
	const WALSegmentName *seg_a = (const WALSegmentName *)a;
	const WALSegmentName *seg_b = (const WALSegmentName *)b;

	/* Sort by timeline first */
	if (seg_a->timeline != seg_b->timeline)
		return (seg_a->timeline < seg_b->timeline) ? -1 : 1;

	/* Then by log_id */
	if (seg_a->log_id != seg_b->log_id)
		return (seg_a->log_id < seg_b->log_id) ? -1 : 1;

	/* Finally by seg_id */
	if (seg_a->seg_id != seg_b->seg_id)
		return (seg_a->seg_id < seg_b->seg_id) ? -1 : 1;

	return 0;
}

/*
 * Scan WAL archive directory
 */
WALArchiveInfo*
scan_wal_archive(const char *wal_archive_dir)
{
	DIR *dir;
	struct dirent *entry;
	WALArchiveInfo *info;
	WALSegmentName *segments = NULL;
	int segment_count = 0;
	int segment_capacity = 1024;  /* Initial capacity */
	WALSegmentName seg;

	if (wal_archive_dir == NULL)
		return NULL;

	/* Allocate WALArchiveInfo structure */
	info = calloc(1, sizeof(WALArchiveInfo));
	if (info == NULL)
		return NULL;

	strncpy(info->archive_path, wal_archive_dir, sizeof(info->archive_path) - 1);

	/* Allocate initial array for segments */
	segments = malloc(segment_capacity * sizeof(WALSegmentName));
	if (segments == NULL)
	{
		free(info);
		return NULL;
	}

	/* Open directory */
	dir = opendir(wal_archive_dir);
	if (dir == NULL)
	{
		log_warning("Cannot open WAL archive directory: %s", wal_archive_dir);
		free(segments);
		free(info);
		return NULL;
	}

	log_debug("Scanning WAL archive: %s", wal_archive_dir);

	/* Scan directory for WAL segment files */
	while ((entry = readdir(dir)) != NULL)
	{
		/* Skip . and .. */
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		/* Try to parse as WAL filename */
		if (!parse_wal_filename(entry->d_name, &seg))
			continue;  /* Not a WAL segment, skip */

		/* Resize array if needed */
		if (segment_count >= segment_capacity)
		{
			segment_capacity *= 2;
			WALSegmentName *new_segments = realloc(segments,
												   segment_capacity * sizeof(WALSegmentName));
			if (new_segments == NULL)
			{
				log_warning("Out of memory while scanning WAL archive");
				break;
			}
			segments = new_segments;
		}

		/* Add segment to array */
		segments[segment_count++] = seg;
	}

	closedir(dir);

	log_debug("Found %d WAL segments", segment_count);

	/* Sort segments by timeline, log_id, seg_id */
	if (segment_count > 0)
	{
		qsort(segments, segment_count, sizeof(WALSegmentName), compare_wal_segments);
	}

	/* Store results */
	info->segments = segments;
	info->segment_count = segment_count;

	return info;
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
