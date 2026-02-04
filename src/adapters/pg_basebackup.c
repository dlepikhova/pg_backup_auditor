/*
 * pg_basebackup.c
 *
 * Adapter for pg_basebackup backups
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
#include <time.h>
#include <sys/stat.h>

/* Forward declarations */
static bool pg_basebackup_detect(const char *path);
static BackupInfo* pg_basebackup_scan(const char *backup_root);
static int pg_basebackup_read_metadata(const char *backup_path, BackupInfo *info);
static ValidationResult* pg_basebackup_validate(BackupInfo *info, WALArchiveInfo *wal);
static void pg_basebackup_cleanup(BackupInfo *info);

/* Helper functions */
static bool is_tar_format(const char *path);
static bool is_plain_format(const char *path);
static int parse_backup_manifest(const char *manifest_path, BackupInfo *info);

/* Adapter definition */
BackupAdapter pg_basebackup_adapter = {
	.name = "pg_basebackup",
	.detect = pg_basebackup_detect,
	.scan = pg_basebackup_scan,
	.read_metadata = pg_basebackup_read_metadata,
	.validate = pg_basebackup_validate,
	.cleanup = pg_basebackup_cleanup
};

/*
 * Helper: Check if path contains tar format backup
 * Looks for any file starting with "base.tar"
 */
static bool
is_tar_format(const char *path)
{
	DIR *dir;
	struct dirent *entry;
	bool found = false;

	dir = opendir(path);
	if (dir == NULL)
		return false;

	while ((entry = readdir(dir)) != NULL)
	{
		/* Check if filename starts with "base.tar" */
		if (strncmp(entry->d_name, "base.tar", 8) == 0)
		{
			found = true;
			break;
		}
	}

	closedir(dir);
	return found;
}

/*
 * Helper: Check if path contains plain format backup
 */
static bool
is_plain_format(const char *path)
{
	char file_path[PATH_MAX];
	bool has_base = false;
	bool has_global = false;
	bool has_marker = false;

	/* Check for base/ directory */
	path_join(file_path, sizeof(file_path), path, "base");
	has_base = is_directory(file_path);

	/* Check for global/ directory */
	path_join(file_path, sizeof(file_path), path, "global");
	has_global = is_directory(file_path);

	/* Check for backup_label or backup_manifest
	 * backup_label - traditional marker (all versions)
	 * backup_manifest - PostgreSQL 13+ with --manifest-checksums
	 * pg_combinebackup removes backup_label but keeps backup_manifest
	 */
	path_join(file_path, sizeof(file_path), path, "backup_label");
	if (file_exists(file_path))
		has_marker = true;

	if (!has_marker)
	{
		path_join(file_path, sizeof(file_path), path, "backup_manifest");
		if (file_exists(file_path))
			has_marker = true;
	}

	/* Valid plain format needs base/, global/, and at least one marker */
	return (has_base && has_global && has_marker);
}

/*
 * Detect if path contains a pg_basebackup backup
 *
 * Supports:
 * - Plain format (directory with backup_label/backup_manifest, base/, global/)
 * - Tar format (base.tar[.gz|.bz2|.lz4|.zst|etc])
 * - pg_combinebackup output (PostgreSQL 17+)
 */
static bool
pg_basebackup_detect(const char *path)
{
	char parent_path[PATH_MAX];
	char control_path[PATH_MAX];
	const char *last_slash;

	/* First check if path exists and is a directory */
	if (!is_directory(path))
		return false;

	/*
	 * Skip detection if parent directory contains backup.control
	 * This prevents detecting pg_probackup's database/ subdirectory as pg_basebackup
	 */
	last_slash = strrchr(path, '/');
	if (last_slash != NULL && last_slash != path)
	{
		size_t parent_len = last_slash - path;
		if (parent_len < sizeof(parent_path))
		{
			memcpy(parent_path, path, parent_len);
			parent_path[parent_len] = '\0';

			path_join(control_path, sizeof(control_path), parent_path, "backup.control");
			if (file_exists(control_path))
			{
				log_debug("Skipping %s - parent has backup.control (pg_probackup backup)", path);
				return false;
			}
		}
	}

	/* Check for tar format - must have file starting with base.tar */
	if (is_tar_format(path))
	{
		log_debug("Detected pg_basebackup tar format at: %s", path);
		return true;
	}

	/* Check for plain format */
	if (is_plain_format(path))
	{
		log_debug("Detected pg_basebackup plain format at: %s", path);
		return true;
	}

	return false;
}

/*
 * Scan a single pg_basebackup backup directory
 *
 * Note: fs_scanner.c already handles directory traversal,
 * so this function processes ONE backup directory, not a tree of backups.
 */
static BackupInfo*
pg_basebackup_scan(const char *backup_path)
{
	BackupInfo *info;
	const char *dir_name;

	log_debug("Scanning pg_basebackup backup: %s", backup_path);

	/* Allocate BackupInfo */
	info = calloc(1, sizeof(BackupInfo));
	if (info == NULL)
	{
		log_error("Failed to allocate memory for BackupInfo");
		return NULL;
	}

	/* Extract directory basename for initial backup_id */
	dir_name = strrchr(backup_path, '/');
	if (dir_name != NULL)
		dir_name++;  /* skip the '/' */
	else
		dir_name = backup_path;

	/* Initialize basic info */
	str_copy(info->backup_id, dir_name, sizeof(info->backup_id));
	str_copy(info->backup_path, backup_path, sizeof(info->backup_path));
	str_copy(info->node_name, "localhost", sizeof(info->node_name));
	info->tool = BACKUP_TOOL_PG_BASEBACKUP;
	info->type = BACKUP_TYPE_FULL;  /* pg_basebackup only does full backups */
	info->status = BACKUP_STATUS_OK;  /* Will be validated later */
	info->next = NULL;

	/* Read metadata to populate remaining fields
	 * This will update backup_id to timestamp format, node_name, timestamps, LSNs, etc.
	 */
	if (pg_basebackup_read_metadata(backup_path, info) != STATUS_OK)
	{
		log_warning("Failed to parse backup metadata at: %s", backup_path);
		info->status = BACKUP_STATUS_ERROR;
		/* Don't return NULL - we still want to show the backup with ERROR status */
	}

	/* Calculate backup size */
	info->data_bytes = get_directory_size(backup_path);
	log_debug("Backup size: %llu bytes", (unsigned long long)info->data_bytes);

	/* Calculate WAL size if present */
	char wal_path[PATH_MAX];
	path_join(wal_path, sizeof(wal_path), backup_path, "pg_wal");
	if (is_directory(wal_path))
	{
		/* Plain format with pg_wal directory */
		info->wal_bytes = get_directory_size(wal_path);
		log_debug("WAL size (pg_wal/): %llu bytes", (unsigned long long)info->wal_bytes);
	}
	else
	{
		/* Check for pg_wal.tar* in tar format */
		DIR *wal_dir = opendir(backup_path);
		if (wal_dir != NULL)
		{
			struct dirent *wal_entry;
			while ((wal_entry = readdir(wal_dir)) != NULL)
			{
				if (strncmp(wal_entry->d_name, "pg_wal.tar", 10) == 0)
				{
					char wal_tar_path[PATH_MAX];
					struct stat wal_st;
					path_join(wal_tar_path, sizeof(wal_tar_path), backup_path, wal_entry->d_name);
					if (stat(wal_tar_path, &wal_st) == 0)
					{
						info->wal_bytes = wal_st.st_size;
						log_debug("WAL size (%s): %llu bytes", wal_entry->d_name, (unsigned long long)info->wal_bytes);
						break;
					}
				}
			}
			closedir(wal_dir);
		}
	}

	/* Set end_time from directory modification time (best approximation) */
	struct stat st;
	if (stat(backup_path, &st) == 0)
	{
		info->end_time = st.st_mtime;
	}

	/* Try to read PG_VERSION file from plain format */
	char version_path[PATH_MAX];
	path_join(version_path, sizeof(version_path), backup_path, "PG_VERSION");

	FILE *ver_fp = fopen(version_path, "r");
	if (ver_fp != NULL)
	{
		char version_str[32];
		if (fgets(version_str, sizeof(version_str), ver_fp) != NULL)
		{
			info->pg_version = atoi(version_str) * 10000;
			log_debug("Read PG_VERSION from file: major=%d", info->pg_version / 10000);
		}
		fclose(ver_fp);
	}
	else
	{
		/* Try to extract PG_VERSION from tar archive */
		DIR *tar_dir;
		struct dirent *tar_entry;

		tar_dir = opendir(backup_path);
		if (tar_dir != NULL)
		{
			while ((tar_entry = readdir(tar_dir)) != NULL)
			{
				if (strncmp(tar_entry->d_name, "base.tar", 8) == 0)
				{
					char tar_file[PATH_MAX];
					char extract_cmd[PATH_MAX * 2];

					path_join(tar_file, sizeof(tar_file), backup_path, tar_entry->d_name);

					/* Determine extraction command based on compression */
					if (strstr(tar_entry->d_name, ".gz") != NULL)
						snprintf(extract_cmd, sizeof(extract_cmd),
								"tar -xzOf '%s' PG_VERSION 2>/dev/null", tar_file);
					else if (strstr(tar_entry->d_name, ".bz2") != NULL)
						snprintf(extract_cmd, sizeof(extract_cmd),
								"tar -xjOf '%s' PG_VERSION 2>/dev/null", tar_file);
					else if (strstr(tar_entry->d_name, ".xz") != NULL || strstr(tar_entry->d_name, ".lz4") != NULL)
						snprintf(extract_cmd, sizeof(extract_cmd),
								"tar -xJOf '%s' PG_VERSION 2>/dev/null", tar_file);
					else
						snprintf(extract_cmd, sizeof(extract_cmd),
								"tar -xOf '%s' PG_VERSION 2>/dev/null", tar_file);

					ver_fp = popen(extract_cmd, "r");
					if (ver_fp != NULL)
					{
						char version_str[32];
						if (fgets(version_str, sizeof(version_str), ver_fp) != NULL)
						{
							info->pg_version = atoi(version_str) * 10000;
							log_debug("Extracted PG_VERSION from tar: major=%d", info->pg_version / 10000);
						}
						pclose(ver_fp);
					}
					break;
				}
			}
			closedir(tar_dir);
		}
	}

	log_debug("Scanned pg_basebackup backup: %s (node: %s)",
			  info->backup_id, info->node_name);

	return info;
}

/*
 * Read metadata from backup_label file
 * Handles both plain format (backup_label as file) and tar format (backup_label inside tar)
 */
static int
pg_basebackup_read_metadata(const char *backup_path, BackupInfo *info)
{
	char label_path[PATH_MAX];
	char tar_path[PATH_MAX];
	char cmd[PATH_MAX * 2];
	FILE *fp = NULL;
	char line[1024];
	bool found_start_time = false;
	bool is_incremental = false;  /* Detect incremental backups */
	bool is_tar = false;
	struct tm tm_time = {0};
	DIR *dir;
	struct dirent *entry;

	/* Build path to backup_label (plain format) */
	path_join(label_path, sizeof(label_path), backup_path, "backup_label");

	/* Try to open backup_label as regular file (plain format) */
	fp = fopen(label_path, "r");

	if (fp == NULL)
	{
		/* backup_label not found as file, check if this is tar format */
		dir = opendir(backup_path);
		if (dir != NULL)
		{
			while ((entry = readdir(dir)) != NULL)
			{
				/* Find file starting with "base.tar" */
				if (strncmp(entry->d_name, "base.tar", 8) == 0)
				{
					/* Found tar archive, extract backup_label */
					path_join(tar_path, sizeof(tar_path), backup_path, entry->d_name);

					/* Determine extraction command based on compression */
					if (strstr(entry->d_name, ".gz") != NULL)
					{
						snprintf(cmd, sizeof(cmd), "tar -xzOf '%s' backup_label 2>/dev/null", tar_path);
					}
					else if (strstr(entry->d_name, ".bz2") != NULL)
					{
						snprintf(cmd, sizeof(cmd), "tar -xjOf '%s' backup_label 2>/dev/null", tar_path);
					}
					else if (strstr(entry->d_name, ".xz") != NULL || strstr(entry->d_name, ".lz4") != NULL)
					{
						snprintf(cmd, sizeof(cmd), "tar -xJOf '%s' backup_label 2>/dev/null", tar_path);
					}
					else
					{
						/* No compression */
						snprintf(cmd, sizeof(cmd), "tar -xOf '%s' backup_label 2>/dev/null", tar_path);
					}

					log_debug("Extracting backup_label from tar: %s", entry->d_name);
					fp = popen(cmd, "r");
					is_tar = true;
					break;
				}
			}
			closedir(dir);
		}

		if (fp == NULL)
		{
			/* backup_label not found, try backup_manifest (pg_combinebackup) */
			log_debug("backup_label not found, trying backup_manifest");

			char manifest_path[PATH_MAX];
			path_join(manifest_path, sizeof(manifest_path), backup_path, "backup_manifest");

			if (parse_backup_manifest(manifest_path, info) == STATUS_OK)
			{
				log_debug("Successfully parsed backup_manifest for pg_combinebackup backup");
				return STATUS_OK;
			}

			log_warning("Neither backup_label nor valid backup_manifest found at: %s", backup_path);
			return STATUS_ERROR;
		}
	}

	/* Parse backup_label line by line */
	while (fgets(line, sizeof(line), fp) != NULL)
	{
		char *value;
		char *newline;

		/* START WAL LOCATION: 0/2000028 (file 000000010000000000000002) */
		if (strncmp(line, "START WAL LOCATION:", 19) == 0)
		{
			value = line + 19;
			while (*value == ' ' || *value == '\t')
				value++;

			/* Parse LSN in format X/X */
			unsigned int hi, lo;
			if (sscanf(value, "%X/%X", &hi, &lo) == 2)
			{
				info->start_lsn = ((uint64_t)hi << 32) | lo;
			}

			/* Extract WAL filename from "(file XXXXXXXXXX)" */
			char *file_start = strstr(value, "(file ");
			if (file_start != NULL)
			{
				file_start += 6;  /* skip "(file " */
				char *file_end = strchr(file_start, ')');
				if (file_end != NULL)
				{
					size_t len = file_end - file_start;
					if (len < sizeof(info->wal_start_file))
					{
						memcpy(info->wal_start_file, file_start, len);
						info->wal_start_file[len] = '\0';
					}
				}
			}
		}
		/* CHECKPOINT LOCATION: 0/2000080 */
		else if (strncmp(line, "CHECKPOINT LOCATION:", 20) == 0)
		{
			value = line + 20;
			while (*value == ' ' || *value == '\t')
				value++;

			/* Parse LSN in format X/X - this is the stop_lsn */
			unsigned int hi, lo;
			if (sscanf(value, "%X/%X", &hi, &lo) == 2)
			{
				info->stop_lsn = ((uint64_t)hi << 32) | lo;
			}
		}
		/* BACKUP METHOD: streamed */
		else if (strncmp(line, "BACKUP METHOD:", 14) == 0)
		{
			value = line + 14;
			while (*value == ' ' || *value == '\t')
				value++;

			/* Remove trailing newline */
			newline = strchr(value, '\n');
			if (newline != NULL)
				*newline = '\0';

			str_copy(info->backup_method, value, sizeof(info->backup_method));
		}
		/* BACKUP FROM: primary */
		else if (strncmp(line, "BACKUP FROM:", 12) == 0)
		{
			value = line + 12;
			while (*value == ' ' || *value == '\t')
				value++;

			/* Remove trailing newline */
			newline = strchr(value, '\n');
			if (newline != NULL)
				*newline = '\0';

			str_copy(info->backup_from, value, sizeof(info->backup_from));
		}
		/* LABEL: pg_basebackup base backup */
		else if (strncmp(line, "LABEL:", 6) == 0)
		{
			value = line + 6;
			while (*value == ' ' || *value == '\t')
				value++;

			/* Remove trailing newline */
			newline = strchr(value, '\n');
			if (newline != NULL)
				*newline = '\0';

			str_copy(info->backup_label, value, sizeof(info->backup_label));
		}
		/* START TIME: 2024-01-08 10:05:30 UTC */
		else if (strncmp(line, "START TIME:", 11) == 0)
		{
			value = line + 11;
			while (*value == ' ' || *value == '\t')
				value++;

			/* Parse timestamp: YYYY-MM-DD HH:MM:SS */
			if (sscanf(value, "%d-%d-%d %d:%d:%d",
					   &tm_time.tm_year, &tm_time.tm_mon, &tm_time.tm_mday,
					   &tm_time.tm_hour, &tm_time.tm_min, &tm_time.tm_sec) == 6)
			{
				tm_time.tm_year -= 1900;  /* years since 1900 */
				tm_time.tm_mon -= 1;      /* 0-11 */
				tm_time.tm_isdst = -1;    /* auto-detect DST */

				info->start_time = mktime(&tm_time);
				found_start_time = true;

				/* Generate backup_id from START TIME: YYYYMMDD-HHMMSS */
				snprintf(info->backup_id, sizeof(info->backup_id),
						 "%04d%02d%02d-%02d%02d%02d",
						 tm_time.tm_year + 1900,
						 tm_time.tm_mon + 1,
						 tm_time.tm_mday,
						 tm_time.tm_hour,
						 tm_time.tm_min,
						 tm_time.tm_sec);
			}
		}
		/* START TIMELINE: 1 */
		else if (strncmp(line, "START TIMELINE:", 15) == 0)
		{
			value = line + 15;
			while (*value == ' ' || *value == '\t')
				value++;

			info->timeline = (TimeLineID)atoi(value);
		}
		/* INCREMENTAL FROM LSN: 0/6000028 (PostgreSQL 17+) */
		else if (strncmp(line, "INCREMENTAL FROM LSN:", 21) == 0)
		{
			is_incremental = true;
			/* Note: We could also parse the parent LSN if needed in the future */
		}
	}

	/* Close file handle (use pclose for tar, fclose for plain) */
	if (is_tar)
		pclose(fp);
	else
		fclose(fp);

	if (!found_start_time)
	{
		log_warning("START TIME not found in backup_label");
		return STATUS_ERROR;
	}

	/* Update backup type if incremental backup was detected */
	if (is_incremental)
	{
		info->type = BACKUP_TYPE_INCREMENTAL;
		log_debug("Detected incremental backup (PostgreSQL 17+)");
	}

	/* Try to extract node name from directory name
	 * Example: "backup_shard1_20240108" -> "shard1"
	 * Pattern: look for common separators and extract meaningful parts
	 */
	const char *dir_name = strrchr(backup_path, '/');
	if (dir_name != NULL)
		dir_name++;  /* skip the '/' */
	else
		dir_name = backup_path;

	/* Simple heuristic: if directory name contains underscore and looks like
	 * it has a node identifier, try to extract it
	 * For now, keep "localhost" as default - more sophisticated parsing can be added later
	 */

	return STATUS_OK;
}

/*
 * Validate pg_basebackup backup
 */
static ValidationResult*
pg_basebackup_validate(BackupInfo *info, WALArchiveInfo *wal)
{
	/* TODO: Implement validation
	 *
	 * Check:
	 * - backup_label exists and is valid
	 * - Required directories exist (base/, global/)
	 * - PG_VERSION exists and is valid
	 * - If WAL archive provided, check WAL availability
	 */

	(void) info;  /* unused for now */
	(void) wal;  /* unused for now */

	return NULL;
}

/*
 * Parse backup_manifest file (minimal JSON parsing)
 * Used for pg_combinebackup backups without backup_label
 */
static int
parse_backup_manifest(const char *manifest_path, BackupInfo *info)
{
	FILE *fp;
	char line[2048];
	bool found_timeline = false;
	bool found_lsn = false;
	time_t now;

	fp = fopen(manifest_path, "r");
	if (fp == NULL)
	{
		log_debug("backup_manifest not found: %s", manifest_path);
		return STATUS_ERROR;
	}

	/* Simple text-based JSON parsing for specific fields */
	while (fgets(line, sizeof(line), fp) != NULL)
	{
		char *ptr;

		/* Look for "Timeline": <number> */
		if (!found_timeline && (ptr = strstr(line, "\"Timeline\"")) != NULL)
		{
			ptr = strchr(ptr, ':');
			if (ptr != NULL)
			{
				info->timeline = (TimeLineID)atoi(ptr + 1);
				found_timeline = true;
			}
		}

		/* Look for "Start-LSN": "X/XXXXXX" */
		if (!found_lsn && (ptr = strstr(line, "\"Start-LSN\"")) != NULL)
		{
			ptr = strchr(ptr, '"');
			if (ptr != NULL)
			{
				ptr++; /* skip opening quote */
				ptr = strchr(ptr, '"');
				if (ptr != NULL)
				{
					ptr++; /* skip second quote */
					ptr = strchr(ptr, '"');
					if (ptr != NULL)
					{
						ptr++; /* now at the LSN value */
						unsigned int hi, lo;
						if (sscanf(ptr, "%X/%X", &hi, &lo) == 2)
						{
							info->start_lsn = ((uint64_t)hi << 32) | lo;
							found_lsn = true;
						}
					}
				}
			}
		}

		if (found_timeline && found_lsn)
			break;
	}

	fclose(fp);

	if (!found_timeline && !found_lsn)
	{
		log_warning("backup_manifest does not contain Timeline or Start-LSN");
		return STATUS_ERROR;
	}

	/* For pg_combinebackup, use current time as fallback for backup_id */
	now = time(NULL);
	struct tm *tm_now = localtime(&now);
	snprintf(info->backup_id, sizeof(info->backup_id),
			 "%04d%02d%02d-%02d%02d%02d",
			 tm_now->tm_year + 1900,
			 tm_now->tm_mon + 1,
			 tm_now->tm_mday,
			 tm_now->tm_hour,
			 tm_now->tm_min,
			 tm_now->tm_sec);

	/* Set start_time to now as well */
	info->start_time = now;

	log_debug("Parsed backup_manifest: timeline=%u, start_lsn=%lX/%X",
			  info->timeline,
			  (unsigned long)(info->start_lsn >> 32),
			  (unsigned int)(info->start_lsn & 0xFFFFFFFF));

	return STATUS_OK;
}

/*
 * Cleanup resources
 */
static void
pg_basebackup_cleanup(BackupInfo *info)
{
	/* TODO: Free any allocated resources */
	(void) info;  /* unused for now */
}
