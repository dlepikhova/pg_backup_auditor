/*
 * pg_basebackup_validator.c
 *
 * Structure validation and embedded WAL access for pg_basebackup backups.
 * Implements the validate_structure and get_embedded_wal adapter hooks.
 *
 * Supported formats:
 *   plain — standard directory layout (base/, global/, pg_wal/)
 *   tar   — base.tar[.gz|.bz2|.xz|.lz4] + pg_wal.tar[.*]
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
 * has_tar_file — check if the backup directory contains a file whose
 * name starts with the given prefix (e.g. "base.tar" or "pg_wal.tar").
 */
static bool
has_tar_file(const char *backup_path, const char *prefix)
{
	DIR           *dir;
	struct dirent *entry;
	bool           found = false;
	size_t         plen  = strlen(prefix);

	dir = opendir(backup_path);
	if (dir == NULL)
		return false;

	while ((entry = readdir(dir)) != NULL)
	{
		if (strncmp(entry->d_name, prefix, plen) == 0)
		{
			found = true;
			break;
		}
	}

	closedir(dir);
	return found;
}

/*
 * find_tar_file — find the full path of the first file whose name starts
 * with the given prefix inside backup_path.  Writes into out (size outsz).
 * Returns true if found.
 */
static bool
find_tar_file(const char *backup_path, const char *prefix,
			  char *out, size_t outsz)
{
	DIR           *dir;
	struct dirent *entry;
	bool           found = false;
	size_t         plen  = strlen(prefix);

	dir = opendir(backup_path);
	if (dir == NULL)
		return false;

	while ((entry = readdir(dir)) != NULL)
	{
		if (strncmp(entry->d_name, prefix, plen) == 0)
		{
			path_join(out, outsz, backup_path, entry->d_name);
			found = true;
			break;
		}
	}

	closedir(dir);
	return found;
}

/* ------------------------------------------------------------------ *
 * pg_basebackup_validate_structure
 *
 * Verifies the on-disk layout of a pg_basebackup backup.
 *
 * Plain format ERROR conditions:
 *   base/               — data directory
 *   global/pg_control   — needed for recovery
 *   backup_label or backup_manifest — at least one must exist
 *
 * Plain format WARNING conditions:
 *   pg_wal/             — missing for stream backups (wal_stream=true)
 *
 * Tar format ERROR conditions:
 *   base.tar*           — main data tarball must exist
 *
 * Tar format WARNING conditions:
 *   pg_wal.tar*         — missing for stream backups (wal_stream=true)
 * ------------------------------------------------------------------ */
ValidationResult*
pg_basebackup_validate_structure(BackupInfo *backup)
{
	ValidationResult *result;
	char              path[PATH_MAX];
	bool              is_tar;

	if (backup == NULL || backup->backup_path[0] == '\0')
		return NULL;

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;
	result->status = BACKUP_STATUS_OK;

	is_tar = has_tar_file(backup->backup_path, "base.tar");

	if (is_tar)
	{
		/* Tar format */
		if (!has_tar_file(backup->backup_path, "base.tar"))
			add_error(result, "Missing base.tar* (main data tarball)");

		if (backup->wal_stream && !has_tar_file(backup->backup_path, "pg_wal.tar"))
			add_warning(result,
						"Missing pg_wal.tar* (expected for stream backup)");
	}
	else
	{
		/* Plain format */
		path_join(path, sizeof(path), backup->backup_path, "base");
		if (!is_directory(path))
			add_error(result, "Missing base/ directory");

		path_join(path, sizeof(path), backup->backup_path, "global");
		path_join(path, sizeof(path), path, "pg_control");
		if (!file_exists(path))
			add_error(result, "Missing global/pg_control");

		/* backup_label or backup_manifest must exist */
		{
			char label[PATH_MAX], manifest[PATH_MAX];
			path_join(label,    sizeof(label),    backup->backup_path, "backup_label");
			path_join(manifest, sizeof(manifest), backup->backup_path, "backup_manifest");
			if (!file_exists(label) && !file_exists(manifest))
				add_error(result,
						  "Missing backup_label and backup_manifest "
						  "(at least one is required)");
		}

		if (backup->wal_stream)
		{
			path_join(path, sizeof(path), backup->backup_path, "pg_wal");
			if (!is_directory(path))
				add_warning(result,
							"Missing pg_wal/ (expected for stream backup)");
		}
	}

	if (result->error_count > 0)
		result->status = BACKUP_STATUS_ERROR;
	else if (result->warning_count > 0)
		result->status = BACKUP_STATUS_WARNING;

	return result;
}

/* ------------------------------------------------------------------ *
 * pg_basebackup_get_embedded_wal
 *
 * For plain format stream backups: scan pg_wal/ directory.
 * For tar format stream backups: scan the directory for pg_wal.tar*
 *   and pass it to scan_wal_archive (which handles tar scanning).
 * Archive-mode backups return NULL.
 * ------------------------------------------------------------------ */
WALArchiveInfo*
pg_basebackup_get_embedded_wal(BackupInfo *backup)
{
	char path[PATH_MAX];

	if (backup == NULL || backup->backup_path[0] == '\0')
		return NULL;

	if (!backup->wal_stream)
		return NULL;

	/* Tar format: pg_wal.tar* */
	if (has_tar_file(backup->backup_path, "base.tar"))
	{
		if (find_tar_file(backup->backup_path, "pg_wal.tar", path, sizeof(path)))
			return scan_wal_archive(path);
		return NULL;
	}

	/* Plain format: pg_wal/ directory */
	path_join(path, sizeof(path), backup->backup_path, "pg_wal");
	if (!is_directory(path))
		return NULL;

	return scan_wal_archive(path);
}
