/*
 * pgbackrest.c
 *
 * pgBackRest adapter implementation
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

#include "pgbackrest.h"
#include "pg_backup_auditor.h"
#include "ini_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <ctype.h>
#include <inttypes.h>

/*
 * Extract JSON value from a JSON string
 * Simple parser for basic key-value extraction
 */
static const char*
get_json_value(const char *json, const char *key)
{
	static char value_buf[256];
	char search_key[128];
	const char *start, *end;
	size_t len;

	if (json == NULL || key == NULL)
		return NULL;

	/* Build search pattern: "key": */
	snprintf(search_key, sizeof(search_key), "\"%s\":", key);

	start = strstr(json, search_key);
	if (start == NULL)
		return NULL;

	/* Skip to value */
	start += strlen(search_key);
	while (*start && isspace((unsigned char)*start))
		start++;

	/* Handle string values (quoted) */
	if (*start == '"')
	{
		start++;
		end = strchr(start, '"');
		if (end == NULL)
			return NULL;
	}
	/* Handle numeric/boolean values */
	else
	{
		end = start;
		while (*end && *end != ',' && *end != '}' && !isspace((unsigned char)*end))
			end++;
	}

	len = end - start;
	if (len >= sizeof(value_buf))
		len = sizeof(value_buf) - 1;

	memcpy(value_buf, start, len);
	value_buf[len] = '\0';

	return value_buf;
}

/*
 * Detect if path is a pgBackRest repository
 */
bool
is_pgbackrest_repo(const char *path)
{
	char backup_path[PATH_MAX];
	char archive_path[PATH_MAX];
	struct stat st;

	if (path == NULL)
		return false;

	/* Check for backup/ directory */
	snprintf(backup_path, sizeof(backup_path), "%s/backup", path);
	if (stat(backup_path, &st) != 0 || !S_ISDIR(st.st_mode))
		return false;

	/* Check for archive/ directory */
	snprintf(archive_path, sizeof(archive_path), "%s/archive", path);
	if (stat(archive_path, &st) != 0 || !S_ISDIR(st.st_mode))
		return false;

	return true;
}

/*
 * Parse backup.manifest file for individual backup details
 */
bool
parse_pgbackrest_manifest(BackupInfo *info, const char *manifest_path)
{
	IniFile *ini;
	const char *value;

	if (info == NULL || manifest_path == NULL)
		return false;

	ini = ini_parse_file(manifest_path);
	if (ini == NULL)
		return false;

	/* Parse [backup] section */
	value = ini_get_value(ini, "backup", "backup-label");
	if (value != NULL && info->backup_id[0] == '\0')
		strncpy(info->backup_id, value, sizeof(info->backup_id) - 1);

	value = ini_get_value(ini, "backup", "backup-type");
	if (value != NULL)
	{
		if (strcmp(value, "full") == 0)
			info->type = BACKUP_TYPE_FULL;
		else if (strcmp(value, "incr") == 0)
			info->type = BACKUP_TYPE_INCREMENTAL;
		else if (strcmp(value, "diff") == 0)
			info->type = BACKUP_TYPE_DELTA;
	}

	value = ini_get_value(ini, "backup", "backup-timestamp-start");
	if (value != NULL)
		info->start_time = (time_t)atoll(value);

	value = ini_get_value(ini, "backup", "backup-timestamp-stop");
	if (value != NULL)
		info->end_time = (time_t)atoll(value);

	value = ini_get_value(ini, "backup", "backup-lsn-start");
	if (value != NULL)
		parse_lsn(value, &info->start_lsn);

	value = ini_get_value(ini, "backup", "backup-lsn-stop");
	if (value != NULL)
		parse_lsn(value, &info->stop_lsn);

	/*
	 * Timeline: stored in the first 8 hex chars of backup-archive-start,
	 * e.g. "000000010000000000000006" → timeline 1.
	 */
	value = ini_get_value(ini, "backup", "backup-archive-start");
	if (value != NULL && strlen(value) >= 8)
	{
		char tl_str[9];
		memcpy(tl_str, value, 8);
		tl_str[8] = '\0';
		info->timeline = (TimeLineID)strtoul(tl_str, NULL, 16);
	}

	/* Parse [backup:db] section for version info */
	value = ini_get_value(ini, "backup:db", "db-version");
	if (value != NULL)
	{
		int major = 0;
		sscanf(value, "%d", &major);
		info->pg_version = major * 10000;
	}

	/* Compression algorithm */
	value = ini_get_value(ini, "backup", "backup-compress-type");
	if (value != NULL)
		snprintf(info->compress_alg, sizeof(info->compress_alg), "%s", value);

	/* backup-size: not always present in pgBackRest manifests.
	 * Only overwrite if the field exists and is non-zero. */
	value = ini_get_value(ini, "backup", "backup-size");
	if (value != NULL)
	{
		uint64_t sz = (uint64_t)atoll(value);
		if (sz > 0)
			info->data_bytes = sz;
	}

	ini_free(ini);
	return true;
}

/*
 * Parse backup.info file for all backups in stanza
 */
BackupInfo*
parse_pgbackrest_backup_info(const char *backup_info_path, const char *stanza_name)
{
	IniFile *ini;
	IniSection *section;
	IniKeyValue *kv;
	BackupInfo *head = NULL;
	BackupInfo *tail = NULL;
	const char *json_value;

	if (backup_info_path == NULL)
		return NULL;

	ini = ini_parse_file(backup_info_path);
	if (ini == NULL)
		return NULL;

	/* Read pgBackRest version from [backrest] section */
	char backrest_version[32] = "";
	const char *ver = ini_get_value(ini, "backrest", "backrest-version");
	if (ver != NULL)
	{
		/* Value may be quoted: "2.51" — strip quotes */
		if (ver[0] == '"')
		{
			snprintf(backrest_version, sizeof(backrest_version), "%s", ver + 1);
			char *end = strchr(backrest_version, '"');
			if (end != NULL)
				*end = '\0';
		}
		else
			snprintf(backrest_version, sizeof(backrest_version), "%s", ver);
	}

	/* Get [backup:current] section which contains all backups */
	section = ini_get_section(ini, "backup:current");
	if (section == NULL)
	{
		ini_free(ini);
		return NULL;
	}

	/* Iterate through all backup entries */
	for (kv = section->first_kv; kv != NULL; kv = kv->next)
	{
		BackupInfo *info;
		char manifest_path[PATH_MAX];
		char *backup_dir;

		/* Allocate new backup info */
		info = calloc(1, sizeof(BackupInfo));
		if (info == NULL)
			continue;

		/* Set backup ID from key name */
		strncpy(info->backup_id, kv->key, sizeof(info->backup_id) - 1);

		/* Set tool type */
		info->tool = BACKUP_TOOL_PGBACKREST;
		info->status = BACKUP_STATUS_OK;

		/* Set tool version from [backrest] section */
		if (backrest_version[0] != '\0')
			snprintf(info->tool_version, sizeof(info->tool_version),
					 "%s", backrest_version);

		/* Parse JSON value for backup metadata */
		json_value = kv->value;

		/* Extract backup type */
		const char *backup_type = get_json_value(json_value, "backup-type");
		if (backup_type != NULL)
		{
			if (strcmp(backup_type, "full") == 0)
				info->type = BACKUP_TYPE_FULL;
			else if (strcmp(backup_type, "incr") == 0)
				info->type = BACKUP_TYPE_INCREMENTAL;
			else if (strcmp(backup_type, "diff") == 0)
				info->type = BACKUP_TYPE_DELTA;
		}

		/* Extract timestamps */
		const char *ts_start = get_json_value(json_value, "backup-timestamp-start");
		if (ts_start != NULL)
			info->start_time = (time_t)atoll(ts_start);

		const char *ts_stop = get_json_value(json_value, "backup-timestamp-stop");
		if (ts_stop != NULL)
			info->end_time = (time_t)atoll(ts_stop);

		/* Extract LSN values */
		const char *lsn_start = get_json_value(json_value, "backup-lsn-start");
		if (lsn_start != NULL)
			parse_lsn(lsn_start, &info->start_lsn);

		const char *lsn_stop = get_json_value(json_value, "backup-lsn-stop");
		if (lsn_stop != NULL)
			parse_lsn(lsn_stop, &info->stop_lsn);

		/* Parent backup (DIFF and INCR backups) */
		const char *prior = get_json_value(json_value, "backup-prior");
		if (prior != NULL)
			strncpy(info->parent_backup_id, prior,
					sizeof(info->parent_backup_id) - 1);

		/* Build backup path */
		backup_dir = strrchr(backup_info_path, '/');
		if (backup_dir != NULL)
		{
			size_t dir_len = backup_dir - backup_info_path;
			snprintf(info->backup_path, sizeof(info->backup_path),
					 "%.*s/%s", (int)dir_len, backup_info_path, info->backup_id);
		}

		/* Calculate backup size from directory */
		if (info->backup_path[0] != '\0')
			info->data_bytes = get_directory_size(info->backup_path);

		/* Try to parse manifest for additional details */
		path_join(manifest_path, sizeof(manifest_path),
				  info->backup_path, "backup.manifest");
		parse_pgbackrest_manifest(info, manifest_path);

		/* Set stanza name if provided */
		if (stanza_name != NULL)
			strncpy(info->instance_name, stanza_name, sizeof(info->instance_name) - 1);

		/* Add to list */
		if (head == NULL)
		{
			head = info;
			tail = info;
		}
		else
		{
			tail->next = info;
			tail = info;
		}
	}

	ini_free(ini);
	return head;
}

/*
 * Scan pgBackRest repository for backups
 */
BackupInfo*
scan_pgbackrest_backups(const char *repo_path)
{
	char backup_base[PATH_MAX];
	DIR *dir;
	struct dirent *entry;
	BackupInfo *all_backups = NULL;
	BackupInfo *tail = NULL;

	if (repo_path == NULL)
		return NULL;

	/* Build path to backup directory */
	snprintf(backup_base, sizeof(backup_base), "%s/backup", repo_path);

	/* Open backup directory */
	dir = opendir(backup_base);
	if (dir == NULL)
		return NULL;

	/* Scan for stanza directories */
	while ((entry = readdir(dir)) != NULL)
	{
		char stanza_path[PATH_MAX];
		char backup_info_path[PATH_MAX];
		struct stat st;
		BackupInfo *stanza_backups;

		/* Skip . and .. */
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;

		/* Build stanza path */
		path_join(stanza_path, sizeof(stanza_path), backup_base, entry->d_name);

		/* Check if it's a directory */
		if (stat(stanza_path, &st) != 0 || !S_ISDIR(st.st_mode))
			continue;

		/* Build backup.info path */
		path_join(backup_info_path, sizeof(backup_info_path),
				  stanza_path, "backup.info");

		/* Check if backup.info exists */
		if (stat(backup_info_path, &st) != 0)
			continue;

		/* Parse backup.info for this stanza */
		stanza_backups = parse_pgbackrest_backup_info(backup_info_path, entry->d_name);

		/* Add to main list */
		if (stanza_backups != NULL)
		{
			if (all_backups == NULL)
			{
				all_backups = stanza_backups;
			}
			else
			{
				tail->next = stanza_backups;
			}

			/* Find new tail */
			tail = stanza_backups;
			while (tail->next != NULL)
				tail = tail->next;
		}
	}

	closedir(dir);
	return all_backups;
}

/*
 * Adapter implementation
 */
static bool
pgbackrest_detect(const char *path)
{
	return is_pgbackrest_repo(path);
}

static BackupInfo*
pgbackrest_scan(const char *path)
{
	return scan_pgbackrest_backups(path);
}

/* Stub implementations for unimplemented adapter methods */
static int pgbackrest_read_metadata_stub(const char *backup_path, BackupInfo *info)
{
	(void) backup_path;
	(void) info;
	return -1;  /* Not implemented */
}

/*
 * pgbackrest_get_wal_archive_path
 *
 * WAL is stored at: <repo>/archive/<stanza>/<pg-version>/
 *
 * backup_path is: <repo>/backup/<stanza>/<label>/
 * Walk up 3 levels to reach <repo>, then descend into archive/<stanza>/.
 * Inside that directory, find the first subdirectory (the pg-version dir)
 * and return it.  If instance_name (stanza) is provided, use it directly.
 */
static char*
pgbackrest_get_wal_archive_path(const char *backup_path, const char *instance_name)
{
	char        repo_path[PATH_MAX];
	char        archive_base[PATH_MAX];
	char        stanza_path[PATH_MAX];
	char        wal_path[PATH_MAX];
	const char *slash;
	size_t      len;
	DIR        *dir;
	struct dirent *entry;
	struct stat st;

	if (backup_path == NULL)
		return NULL;

	/*
	 * Derive <repo> from backup_path:
	 * backup_path = <repo>/backup/<stanza>/<label>
	 * Strip last 3 path components.
	 */
	strncpy(repo_path, backup_path, sizeof(repo_path) - 1);
	repo_path[sizeof(repo_path) - 1] = '\0';

	for (int i = 0; i < 3; i++)
	{
		slash = strrchr(repo_path, '/');
		if (slash == NULL || slash == repo_path)
			return NULL;
		len = (size_t)(slash - repo_path);
		repo_path[len] = '\0';
	}

	/* <repo>/archive/ */
	path_join(archive_base, sizeof(archive_base), repo_path, "archive");
	if (!is_directory(archive_base))
	{
		log_debug("pgBackRest archive directory not found: %s", archive_base);
		return NULL;
	}

	/* <repo>/archive/<stanza>/ */
	if (instance_name != NULL && instance_name[0] != '\0')
	{
		path_join(stanza_path, sizeof(stanza_path), archive_base, instance_name);
	}
	else
	{
		/* No stanza name — pick the first directory in archive/ */
		dir = opendir(archive_base);
		if (dir == NULL)
			return NULL;

		stanza_path[0] = '\0';
		while ((entry = readdir(dir)) != NULL)
		{
			if (entry->d_name[0] == '.')
				continue;
			path_join(stanza_path, sizeof(stanza_path), archive_base, entry->d_name);
			if (stat(stanza_path, &st) == 0 && S_ISDIR(st.st_mode))
				break;
			stanza_path[0] = '\0';
		}
		closedir(dir);

		if (stanza_path[0] == '\0')
			return NULL;
	}

	if (!is_directory(stanza_path))
		return NULL;

	/* <repo>/archive/<stanza>/<pg-version>/ — pick the first versioned dir */
	dir = opendir(stanza_path);
	if (dir == NULL)
		return NULL;

	wal_path[0] = '\0';
	while ((entry = readdir(dir)) != NULL)
	{
		if (entry->d_name[0] == '.')
			continue;
		path_join(wal_path, sizeof(wal_path), stanza_path, entry->d_name);
		if (stat(wal_path, &st) == 0 && S_ISDIR(st.st_mode))
			break;
		wal_path[0] = '\0';
	}
	closedir(dir);

	if (wal_path[0] == '\0')
		return NULL;

	/*
	 * pgBackRest stores WAL in subdirectories with checksum-suffixed names
	 * (e.g. 000000010000000000000006-<hash>.gz), which scan_wal_archive()
	 * cannot parse.  Return NULL so callers skip WAL checks rather than
	 * producing false-positive "missing segment" errors.
	 */
	log_debug("pgBackRest WAL archive found at %s (subdirectory format, skipping scan)",
			  wal_path);
	return NULL;
}


static void pgbackrest_cleanup_stub(BackupInfo *info)
{
	(void) info;
	/* Nothing to clean up yet */
}

/* Implemented in src/validator/pgbackrest_validator.c */
ValidationResult* pgbackrest_validate_structure(BackupInfo *backup);

BackupAdapter pgbackrest_adapter = {
	.name = "pgBackRest",
	.detect = pgbackrest_detect,
	.scan = pgbackrest_scan,
	.read_metadata = pgbackrest_read_metadata_stub,
	.get_wal_archive_path = pgbackrest_get_wal_archive_path,
	.validate_structure = pgbackrest_validate_structure,
	.get_embedded_wal   = NULL,
	.cleanup = pgbackrest_cleanup_stub
};

BackupAdapter*
get_pgbackrest_adapter(void)
{
	return &pgbackrest_adapter;
}
