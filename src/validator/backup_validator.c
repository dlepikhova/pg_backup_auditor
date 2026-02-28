/*
 * backup_validator.c
 *
 * Backup validation logic
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
#include <inttypes.h>
#include <stdint.h>

/*
 * Add error message to validation result
 */
static void
add_error(ValidationResult *result, const char *message)
{
	if (result == NULL || message == NULL)
		return;

	result->errors = realloc(result->errors, sizeof(char *) * (result->error_count + 1));
	if (result->errors == NULL)
		return;

	result->errors[result->error_count] = strdup(message);
	result->error_count++;
}

/*
 * Add warning message to validation result
 */
static void
add_warning(ValidationResult *result, const char *message)
{
	if (result == NULL || message == NULL)
		return;

	result->warnings = realloc(result->warnings, sizeof(char *) * (result->warning_count + 1));
	if (result->warnings == NULL)
		return;

	result->warnings[result->warning_count] = strdup(message);
	result->warning_count++;
}

/*
 * Validate backup metadata
 */
ValidationResult*
validate_backup_metadata(BackupInfo *info)
{
	ValidationResult *result;
	char msg[PATH_MAX + 100];

	if (info == NULL)
		return NULL;

	/* Allocate result structure */
	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;

	result->status = BACKUP_STATUS_OK;
	result->error_count = 0;
	result->warning_count = 0;
	result->errors = NULL;
	result->warnings = NULL;

	/* Check required fields */
	if (info->backup_id[0] == '\0')
		add_error(result, "Missing backup_id");

	if (info->backup_path[0] == '\0')
		add_error(result, "Missing backup_path");

	/* Check backup path exists */
	if (info->backup_path[0] != '\0' && !is_directory(info->backup_path))
	{
		snprintf(msg, sizeof(msg), "Backup path does not exist: %s", info->backup_path);
		add_error(result, msg);
	}

	/* Check timestamps */
	if (info->start_time == 0)
		add_warning(result, "Missing start_time");

	if (info->end_time == 0 && info->status == BACKUP_STATUS_OK)
		add_warning(result, "Missing end_time for completed backup");

	if (info->start_time > 0 && info->end_time > 0 && info->start_time > info->end_time)
	{
		snprintf(msg, sizeof(msg), "Invalid timestamps: start_time (%ld) > end_time (%ld)",
				 (long)info->start_time, (long)info->end_time);
		add_error(result, msg);
	}

	/* Check LSN values */
	if (info->start_lsn > 0 && info->stop_lsn > 0 && info->start_lsn >= info->stop_lsn)
	{
		snprintf(msg, sizeof(msg), "Invalid LSN range: start_lsn (%" PRIu64 ") >= stop_lsn (%" PRIu64 ")",
				 info->start_lsn, info->stop_lsn);
		add_error(result, msg);
	}

	/* Check timeline */
	if (info->timeline == 0)
		add_warning(result, "Missing timeline ID");

	/* Check PostgreSQL version */
	if (info->pg_version == 0)
		add_warning(result, "Missing PostgreSQL version");

	/* Determine final status */
	if (result->error_count > 0)
		result->status = BACKUP_STATUS_ERROR;
	else if (result->warning_count > 0)
		result->status = BACKUP_STATUS_WARNING;
	else
		result->status = BACKUP_STATUS_OK;

	/* Print validation results */
	if (result->error_count > 0)
	{
		printf("  %s[ERROR]%s Metadata validation failed:\n",
			   use_color ? COLOR_RED : "", use_color ? COLOR_RESET : "");
		for (int i = 0; i < result->error_count; i++)
			printf("          %s\n", result->errors[i]);
	}
	if (result->warning_count > 0)
	{
		printf("  %s[WARNING]%s Metadata issues:\n",
			   use_color ? COLOR_YELLOW : "", use_color ? COLOR_RESET : "");
		for (int i = 0; i < result->warning_count; i++)
			printf("            %s\n", result->warnings[i]);
	}
	if (result->error_count == 0 && result->warning_count == 0)
	{
		printf("  %s[OK]%s Metadata validation passed\n",
			   use_color ? COLOR_GREEN : "", use_color ? COLOR_RESET : "");
	}

	return result;
}

/*
 * Validate incremental backup chain
 */
ValidationResult*
validate_backup_chain(BackupInfo *backup, BackupInfo *all_backups)
{
	/* TODO: Implement chain validation
	 *
	 * For incremental backups:
	 * - Find parent backup by parent_backup_id
	 * - Check parent exists and has status OK
	 * - Check parent is FULL or valid incremental
	 * - Recursively validate parent chain
	 * - Check LSN continuity
	 */

	(void) backup;  /* unused for now */
	(void) all_backups;  /* unused for now */

	return NULL;
}

/*
 * Check retention policy compliance
 */
ValidationResult*
check_retention_policy(BackupInfo *backups, int retention_days, int retention_weekly)
{
	/* TODO: Implement retention policy check
	 *
	 * - Count valid backups in last N days
	 * - Count valid weekly backups
	 * - Compare with required minimums
	 * - Return validation result with warnings/errors
	 */

	(void) backups;  /* unused for now */
	(void) retention_days;  /* unused for now */
	(void) retention_weekly;  /* unused for now */

	return NULL;
}

/*
 * Free validation result
 */
void
free_validation_result(ValidationResult *result)
{
	int i;

	if (result == NULL)
		return;

	if (result->errors != NULL)
	{
		for (i = 0; i < result->error_count; i++)
		{
			if (result->errors[i] != NULL)
				free(result->errors[i]);
		}
		free(result->errors);
	}

	if (result->warnings != NULL)
	{
		for (i = 0; i < result->warning_count; i++)
		{
			if (result->warnings[i] != NULL)
				free(result->warnings[i]);
		}
		free(result->warnings);
	}

	free(result);
}

/*
 * Extract a string value from a JSON line of the form:
 *   {"key":"value", ...}
 * Handles both quoted string values ("value") and bare numeric values (123).
 */
static bool
json_get_string(const char *json, const char *key, char *out, size_t outsize)
{
	char search[128];
	const char *p, *end;
	size_t len;

	/* Try "key":"value" (quoted) */
	snprintf(search, sizeof(search), "\"%s\":\"", key);
	p = strstr(json, search);
	if (p != NULL)
	{
		p += strlen(search);
		end = strchr(p, '"');
		if (end == NULL)
			return false;
		len = (size_t)(end - p);
		if (len >= outsize) len = outsize - 1;
		memcpy(out, p, len);
		out[len] = '\0';
		return true;
	}

	/* Try "key":value (bare number) */
	snprintf(search, sizeof(search), "\"%s\":", key);
	p = strstr(json, search);
	if (p != NULL)
	{
		p += strlen(search);
		end = p;
		while (*end && *end != ',' && *end != '}')
			end++;
		len = (size_t)(end - p);
		if (len >= outsize) len = outsize - 1;
		memcpy(out, p, len);
		out[len] = '\0';
		return true;
	}

	return false;
}

/*
 * check_backup_checksums - verify file-level checksums for a backup
 *
 * Parses backup_content.control (one JSON object per line) and for each
 * regular file entry verifies:
 *   1. The file exists in the backup's database/ directory
 *   2. Its size matches the stored "size" field
 *   3. Its CRC32C matches the stored "crc" field (only when compress_alg=none)
 *
 * Returns NULL if backup_content.control is not present (tool not supported).
 */
ValidationResult*
check_backup_checksums(BackupInfo *backup)
{
	char             content_path[PATH_MAX];
	char             db_dir[PATH_MAX];
	char             file_path[PATH_MAX];
	FILE            *fp;
	char             line[8192];
	ValidationResult *result;
	char             msg[PATH_MAX + 128];
	int              checked = 0;
	int              skipped_compressed = 0;

	if (backup == NULL || backup->backup_path[0] == '\0')
		return NULL;

	/* backup_content.control lives directly in the backup directory */
	path_join(content_path, sizeof(content_path),
			  backup->backup_path, "backup_content.control");

	if (!file_exists(content_path))
		return NULL;  /* Tool doesn't produce this file — nothing to check */

	fp = fopen(content_path, "r");
	if (fp == NULL)
		return NULL;

	/* Data files are under <backup_path>/database/ */
	path_join(db_dir, sizeof(db_dir), backup->backup_path, "database");

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
	{
		fclose(fp);
		return NULL;
	}
	result->status = BACKUP_STATUS_OK;

	while (fgets(line, sizeof(line), fp) != NULL)
	{
		char kind[16]         = {0};
		char rel_path[PATH_MAX] = {0};
		char size_str[32]     = {0};
		char crc_str[32]      = {0};
		char compress[32]     = {0};

		if (!json_get_string(line, "kind", kind, sizeof(kind)))
			continue;
		if (strcmp(kind, "reg") != 0)
			continue;  /* skip directories and other entries */

		if (!json_get_string(line, "path", rel_path, sizeof(rel_path)))
			continue;
		if (!json_get_string(line, "size", size_str, sizeof(size_str)))
			continue;
		if (!json_get_string(line, "crc", crc_str, sizeof(crc_str)))
			continue;
		json_get_string(line, "compress_alg", compress, sizeof(compress));

		/* Build full path */
		path_join(file_path, sizeof(file_path), db_dir, rel_path);

		/* Zero-size files are not stored physically by pg_probackup — skip */
		if (strcmp(size_str, "0") == 0)
			continue;

		/* For incremental backups (DELTA/PAGE/PTRACK): only check files
		 * that are physically present in this backup's database/ directory.
		 * Missing files belong to a parent backup and are expected to be absent. */
		if (backup->type != BACKUP_TYPE_FULL && !file_exists(file_path))
			continue;

		/* 1. Existence check (FULL backups: all non-zero files must be present) */
		if (!file_exists(file_path))
		{
			snprintf(msg, sizeof(msg), "Missing file: %s", rel_path);
			add_error(result, msg);
			continue;
		}

		/* 2. Size check */
		off_t stored_size = (off_t) strtoull(size_str, NULL, 10);
		off_t actual_size = get_file_size(file_path);
		if (actual_size != stored_size)
		{
			snprintf(msg, sizeof(msg),
					 "Size mismatch for %s: stored=%lld, actual=%lld",
					 rel_path, (long long)stored_size, (long long)actual_size);
			add_error(result, msg);
			continue;
		}

		/* 3. CRC32C check (only for uncompressed files) */
		/* pg_probackup modifies global/pg_control after writing backup_content.control,
		 * so its stored CRC never matches the file in the backup directory. Skip it. */
		if (strcmp(rel_path, "global/pg_control") == 0)
		{
			checked++;
			continue;
		}

		if (strcmp(compress, "none") == 0 || compress[0] == '\0')
		{
			uint32_t stored_crc = (uint32_t) strtoul(crc_str, NULL, 10);
			uint32_t actual_crc = 0;

			if (!compute_file_crc32c(file_path, &actual_crc))
			{
				snprintf(msg, sizeof(msg), "Cannot read file for CRC check: %s", rel_path);
				add_error(result, msg);
				continue;
			}

			if (actual_crc != stored_crc)
			{
				snprintf(msg, sizeof(msg),
						 "CRC32C mismatch for %s: stored=0x%08X, actual=0x%08X",
						 rel_path, stored_crc, actual_crc);
				add_error(result, msg);
			}
		}
		else
		{
			skipped_compressed++;
		}

		checked++;
	}

	fclose(fp);

	log_debug("Checksum check: %d files verified, %d compressed skipped, %d errors",
			  checked, skipped_compressed, result->error_count);

	return result;
}
