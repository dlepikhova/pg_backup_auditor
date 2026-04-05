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
#include "ini_parser.h"
#include "sha256.h"
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <stdio.h>
#include <strings.h>

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

	/*
	 * For plain (uncompressed) backups pg_data/ is a real directory and we
	 * can verify critical files inside it.  Compressed variants (pg_data.gz
	 * etc.) are opaque — skip inner checks for them.
	 */
	{
		char pg_data[PATH_MAX];
		path_join(pg_data, sizeof(pg_data), backup->backup_path, "pg_data");
		if (is_directory(pg_data))
		{
			/* pg_data/global/pg_control */
			char ctrl[PATH_MAX];
			path_join(ctrl, sizeof(ctrl), pg_data, "global");
			path_join(ctrl, sizeof(ctrl), ctrl, "pg_control");
			if (!file_exists(ctrl))
				add_warning(result, "Missing pg_data/global/pg_control");

			/* pg_data/PG_VERSION */
			path_join(path, sizeof(path), pg_data, "PG_VERSION");
			if (!file_exists(path))
				add_warning(result, "Missing pg_data/PG_VERSION");
		}
	}

	/*
	 * Check backup-error field in backup.manifest.
	 * pgBackRest writes "backup-error=y" when the backup completed with errors
	 * (e.g. modified files during backup).  Such a backup may be unusable.
	 */
	{
		char   manifest_path[PATH_MAX];
		FILE  *fp;
		char   line[INI_MAX_LINE];

		path_join(manifest_path, sizeof(manifest_path),
				  backup->backup_path, "backup.manifest");
		fp = fopen(manifest_path, "r");
		if (fp != NULL)
		{
			bool in_backup_section = false;
			while (fgets(line, sizeof(line), fp) != NULL)
			{
				/* Detect [backup] section */
				if (line[0] == '[')
				{
					in_backup_section = (strncmp(line, "[backup]", 8) == 0);
					if (!in_backup_section && line[1] != 'b')
						break;   /* past [backup] section */
					continue;
				}
				if (!in_backup_section)
					continue;

				if (strncmp(line, "backup-error=y", 14) == 0)
				{
					add_error(result,
							  "backup.manifest reports backup-error=y "
							  "(backup completed with errors — may not be restorable)");
					break;
				}
			}
			fclose(fp);
		}
	}

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

/* ------------------------------------------------------------------ *
 * pgbackrest_check_manifest_checksums
 *
 * Validates per-file SHA1 checksums stored in the [target:file] section
 * of backup.manifest.
 *
 * Each entry has the form:
 *   pg_data/some/file={"checksum":"<sha1hex>","size":<n>,...}
 *
 * For every file that has a "checksum" field:
 *   - verify the file exists at backup_path/key
 *   - compute SHA1 and compare
 *
 * Files without "checksum" (checksum-page=true block files where pgbackrest
 * stores page-level checksums separately, or zero-size files) are skipped —
 * their integrity is covered by PostgreSQL's own page checksums.
 *
 * Returns NULL if backup.manifest does not exist or backup is not pgbackrest.
 * Only works for plain (uncompressed) backups — compressed data files are
 * opaque without unpacking.
 * ------------------------------------------------------------------ */

/*
 * Extract a JSON string value: {"checksum":"<value>",...}
 * Returns pointer into a static buffer, or NULL if not found.
 */
static const char *
extract_json_str(const char *json, const char *key)
{
	static char buf[256];
	char        search[64];
	const char *p, *q;
	size_t      len;

	snprintf(search, sizeof(search), "\"%s\":\"", key);
	p = strstr(json, search);
	if (p == NULL)
		return NULL;

	p += strlen(search);
	q  = strchr(p, '"');
	if (q == NULL)
		return NULL;

	len = (size_t)(q - p);
	if (len >= sizeof(buf))
		len = sizeof(buf) - 1;
	memcpy(buf, p, len);
	buf[len] = '\0';
	return buf;
}

/*
 * Compute SHA1 of a file.  Returns true on success.
 * We reuse our SHA256 infrastructure but pgbackrest uses SHA1 (40 hex chars).
 * Since we don't have a standalone SHA1 implementation, we shell out to
 * shasum/sha1sum — portable across macOS and Linux.
 */
static bool
compute_file_sha1(const char *path, char *out_hex, size_t out_sz)
{
	/*
	 * Try shasum -a 1 (macOS, also available on Linux via Perl)
	 * then fall back to sha1sum (coreutils, Linux/FreeBSD).
	 * Output format for both: "<40-hex-chars>  filename\n"
	 */
	static const char * const cmdfmt[] = {
		"shasum -a 1 -- '%s' 2>/dev/null",
		"sha1sum -- '%s' 2>/dev/null",
		NULL
	};
	char   cmd[PATH_MAX + 32];
	FILE  *fp;
	char   line[256];
	size_t len;
	int    i;

	if (out_sz < 41)
		return false;

	for (i = 0; cmdfmt[i] != NULL; i++)
	{
		snprintf(cmd, sizeof(cmd), cmdfmt[i], path);
		fp = popen(cmd, "r");
		if (fp == NULL)
			continue;

		line[0] = '\0';
		if (fgets(line, sizeof(line), fp) == NULL)
			line[0] = '\0';
		pclose(fp);

		/* Verify we got 40 hex chars */
		len = 0;
		while (len < 40 && line[len] != '\0' &&
			   ((line[len] >= '0' && line[len] <= '9') ||
				(line[len] >= 'a' && line[len] <= 'f') ||
				(line[len] >= 'A' && line[len] <= 'F')))
			len++;

		if (len == 40)
		{
			memcpy(out_hex, line, 40);
			out_hex[40] = '\0';
			return true;
		}
	}

	return false;
}

ValidationResult *
pgbackrest_check_manifest_checksums(BackupInfo *backup)
{
	ValidationResult *result;
	char              manifest_path[PATH_MAX];
	IniFile          *ini;
	IniSection       *section;
	IniKeyValue      *kv;
	char              msg[PATH_MAX + 128];
	bool              is_plain;

	if (backup == NULL || backup->backup_path[0] == '\0')
		return NULL;

	if (backup->tool != BACKUP_TOOL_PGBACKREST)
		return NULL;

	path_join(manifest_path, sizeof(manifest_path),
			  backup->backup_path, "backup.manifest");
	if (!file_exists(manifest_path))
		return NULL;

	/* Only plain backups: pg_data/ must be a real directory */
	{
		char pg_data[PATH_MAX];
		path_join(pg_data, sizeof(pg_data), backup->backup_path, "pg_data");
		is_plain = is_directory(pg_data);
	}
	if (!is_plain)
		return NULL;   /* compressed — cannot check without unpacking */

	ini = ini_parse_file(manifest_path);
	if (ini == NULL)
		return NULL;

	section = ini_get_section(ini, "target:file");
	if (section == NULL)
	{
		ini_free(ini);
		return NULL;
	}

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
	{
		ini_free(ini);
		return NULL;
	}
	result->status = BACKUP_STATUS_OK;

	for (kv = section->first_kv; kv != NULL; kv = kv->next)
	{
		const char *rel_path  = kv->key;   /* e.g. "pg_data/PG_VERSION" */
		const char *json      = kv->value;
		const char *checksum;
		char        file_path[PATH_MAX];
		char        actual[41];

		/* Skip pg_wal entries — covered by WAL validation */
		if (strncmp(rel_path, "pg_data/pg_wal/", 15) == 0 ||
			strcmp(rel_path,  "pg_data/pg_wal")  == 0)
			continue;

		checksum = extract_json_str(json, "checksum");
		if (checksum == NULL)
			continue;   /* no checksum field: zero-size or page-checksum file */

		path_join(file_path, sizeof(file_path),
				  backup->backup_path, rel_path);

		if (!file_exists(file_path))
		{
			snprintf(msg, sizeof(msg), "Missing file: %s", rel_path);
			add_error(result, msg);
			continue;
		}

		if (!compute_file_sha1(file_path, actual, sizeof(actual)))
		{
			snprintf(msg, sizeof(msg),
					 "Cannot compute SHA1 for: %s", rel_path);
			add_warning(result, msg);
			continue;
		}

		if (strcasecmp(actual, checksum) != 0)
		{
			snprintf(msg, sizeof(msg),
					 "SHA1 mismatch: %s (expected %.16s…, got %.16s…)",
					 rel_path, checksum, actual);
			add_error(result, msg);
		}
	}

	ini_free(ini);

	if (result->error_count > 0)
		result->status = BACKUP_STATUS_ERROR;
	else if (result->warning_count > 0)
		result->status = BACKUP_STATUS_WARNING;

	return result;
}
