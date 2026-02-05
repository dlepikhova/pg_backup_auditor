/*
 * backup_validator.c
 *
 * Backup validation logic
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
#include <inttypes.h>

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

	if (info->start_time > 0 && info->end_time > 0 && info->start_time >= info->end_time)
	{
		snprintf(msg, sizeof(msg), "Invalid timestamps: start_time (%ld) >= end_time (%ld)",
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
