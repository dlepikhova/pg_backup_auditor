/*
 * pgbackrest_validator.c
 *
 * Structure validation for pgBackRest backups.
 * Implements the validate_structure adapter hook.
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
#include <dirent.h>

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

/*
 * has_pg_data — check that the backup directory contains a pg_data/
 * subdirectory (uncompressed) or any file starting with "pg_data"
 * (compressed variants: pg_data.gz, pg_data.xz, etc.).
 */
static bool
has_pg_data(const char *backup_path)
{
	char       path[PATH_MAX];
	DIR       *dir;
	struct dirent *entry;
	bool       found = false;

	/* Plain directory */
	path_join(path, sizeof(path), backup_path, "pg_data");
	if (is_directory(path))
		return true;

	/* Compressed archive variants */
	dir = opendir(backup_path);
	if (dir == NULL)
		return false;

	while ((entry = readdir(dir)) != NULL)
	{
		if (strncmp(entry->d_name, "pg_data", 7) == 0)
		{
			found = true;
			break;
		}
	}
	closedir(dir);
	return found;
}

/* ------------------------------------------------------------------ *
 * pgbackrest_validate_structure
 *
 * Verifies the on-disk layout of a pgBackRest backup directory.
 *
 * backup_path points to the individual backup label directory, e.g.:
 *   <repo>/backup/<stanza>/20240101-120000F/
 *
 * ERROR conditions:
 *   backup.manifest   — always written; without it recovery is impossible
 *   pg_data[.*]       — data directory (plain or compressed)
 *
 * WARNING conditions:
 *   backup.manifest.copy — redundant copy; absence does not block restore
 * ------------------------------------------------------------------ */
ValidationResult*
pgbackrest_validate_structure(BackupInfo *backup)
{
	ValidationResult *result;
	char              path[PATH_MAX];

	if (backup == NULL || backup->backup_path[0] == '\0')
		return NULL;

	if (backup->tool != BACKUP_TOOL_PGBACKREST)
		return NULL;

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;
	result->status = BACKUP_STATUS_OK;

	/* backup.manifest */
	path_join(path, sizeof(path), backup->backup_path, "backup.manifest");
	if (!file_exists(path))
		add_error(result, "Missing backup.manifest");

	/* pg_data/ or pg_data.* */
	if (!has_pg_data(backup->backup_path))
		add_error(result, "Missing pg_data directory or archive");

	/* backup.manifest.copy (redundant copy, absence is a soft warning) */
	path_join(path, sizeof(path), backup->backup_path, "backup.manifest.copy");
	if (!file_exists(path))
		add_warning(result,
					"Missing backup.manifest.copy "
					"(redundant copy absent — not critical)");

	if (result->error_count > 0)
		result->status = BACKUP_STATUS_ERROR;
	else if (result->warning_count > 0)
		result->status = BACKUP_STATUS_WARNING;

	return result;
}
