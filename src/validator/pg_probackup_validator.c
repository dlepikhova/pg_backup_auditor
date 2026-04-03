/*
 * pg_probackup_validator.c
 *
 * Structure validation and embedded WAL access for pg_probackup backups.
 * Implements the validate_structure and get_embedded_wal adapter hooks.
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
#include <stdlib.h>
#include <string.h>

static void
add_error(ValidationResult *result, const char *message)
{
	if (result == NULL || message == NULL)
		return;
	result->errors = realloc(result->errors,
							 sizeof(char *) * (result->error_count + 1));
	if (result->errors == NULL)
		return;
	result->errors[result->error_count] = strdup(message);
	result->error_count++;
}

static void
add_warning(ValidationResult *result, const char *message)
{
	if (result == NULL || message == NULL)
		return;
	result->warnings = realloc(result->warnings,
							   sizeof(char *) * (result->warning_count + 1));
	if (result->warnings == NULL)
		return;
	result->warnings[result->warning_count] = strdup(message);
	result->warning_count++;
}

/* ------------------------------------------------------------------ *
 * pg_probackup_validate_structure
 *
 * Verifies the on-disk layout of a pg_probackup backup.
 * Does NOT read file contents — only checks presence of required paths.
 *
 * ERROR conditions (backup cannot be restored without these):
 *   database/                 — data directory
 *   database/database_map     — OID→dbname map (always written)
 *   database/global/pg_control
 *   database/backup_label     — ARCHIVE mode only (stream=false)
 *
 * WARNING conditions (degraded functionality):
 *   database/PG_VERSION       — FULL backups only (DELTA/PTRACK skip it)
 *   backup_content.control    — checksum validation impossible
 *   database/pg_wal/          — missing for STREAM backups (stream=true)
 * ------------------------------------------------------------------ */
ValidationResult*
pg_probackup_validate_structure(BackupInfo *backup)
{
	ValidationResult *result;
	char              path[PATH_MAX];

	if (backup == NULL || backup->backup_path[0] == '\0')
		return NULL;

	if (backup->tool != BACKUP_TOOL_PG_PROBACKUP)
		return NULL;

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;
	result->status = BACKUP_STATUS_OK;

	/* database/ */
	path_join(path, sizeof(path), backup->backup_path, "database");
	if (!is_directory(path))
	{
		add_error(result, "Missing database/ directory");
	}
	else
	{
		/* database/database_map */
		path_join(path, sizeof(path), backup->backup_path, "database");
		path_join(path, sizeof(path), path, "database_map");
		if (!file_exists(path))
			add_error(result, "Missing database/database_map");

		/* database/global/pg_control */
		{
			char ctrl[PATH_MAX];
			path_join(ctrl, sizeof(ctrl), backup->backup_path, "database");
			path_join(ctrl, sizeof(ctrl), ctrl, "global");
			path_join(ctrl, sizeof(ctrl), ctrl, "pg_control");
			if (!file_exists(ctrl))
				add_error(result, "Missing database/global/pg_control");
		}

		/* database/backup_label (ARCHIVE mode only) */
		if (!backup->wal_stream)
		{
			path_join(path, sizeof(path), backup->backup_path, "database");
			path_join(path, sizeof(path), path, "backup_label");
			if (!file_exists(path))
				add_error(result,
						  "Missing database/backup_label "
						  "(required for archive-mode backup)");
		}

		/* database/PG_VERSION — only present in FULL backups;
		 * pg_probackup skips unchanged files in DELTA/PAGE/PTRACK */
		if (backup->type == BACKUP_TYPE_FULL)
		{
			path_join(path, sizeof(path), backup->backup_path, "database");
			path_join(path, sizeof(path), path, "PG_VERSION");
			if (!file_exists(path))
				add_warning(result, "Missing database/PG_VERSION");
		}

		/* database/pg_wal/ (STREAM mode) */
		if (backup->wal_stream)
		{
			char wal_dir[PATH_MAX];
			path_join(wal_dir, sizeof(wal_dir), backup->backup_path, "database");
			path_join(wal_dir, sizeof(wal_dir), wal_dir, "pg_wal");
			if (!is_directory(wal_dir))
				add_warning(result,
							"Missing database/pg_wal/ "
							"(expected for stream backup)");
		}

		/*
		 * tablespace_map is absent when no non-default tablespaces exist,
		 * so its absence is not an error.
		 *
		 * TODO: parse tablespace_map entries and verify that the referenced
		 * pg_tblspc/<OID> subdirectories exist inside the backup.
		 * TODO: parse database_map JSON and verify base/<OID>/ directories.
		 */
	}

	/* backup_content.control */
	path_join(path, sizeof(path),
			  backup->backup_path, "backup_content.control");
	if (!file_exists(path))
		add_warning(result,
					"Missing backup_content.control "
					"(checksum validation not possible)");

	if (result->error_count > 0)
		result->status = BACKUP_STATUS_ERROR;
	else if (result->warning_count > 0)
		result->status = BACKUP_STATUS_WARNING;

	return result;
}

/* ------------------------------------------------------------------ *
 * pg_probackup_get_embedded_wal
 *
 * For pg_probackup stream backups, WAL is stored in database/pg_wal/.
 * Returns a WALArchiveInfo* for that directory, or NULL if not present.
 * Archive-mode backups return NULL (WAL comes from external archive).
 * ------------------------------------------------------------------ */
WALArchiveInfo*
pg_probackup_get_embedded_wal(BackupInfo *backup)
{
	char pg_wal_path[PATH_MAX];

	if (backup == NULL || backup->backup_path[0] == '\0')
		return NULL;

	if (!backup->wal_stream)
		return NULL;

	path_join(pg_wal_path, sizeof(pg_wal_path), backup->backup_path, "database");
	path_join(pg_wal_path, sizeof(pg_wal_path), pg_wal_path, "pg_wal");

	if (!is_directory(pg_wal_path))
		return NULL;

	return scan_wal_archive(pg_wal_path);
}
