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

	/* Parse [backup:db] section for version info */
	value = ini_get_value(ini, "backup:db", "db-version");
	if (value != NULL)
	{
		int major = 0;
		sscanf(value, "%d", &major);
		info->pg_version = major * 10000;
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

		/* Build backup path */
		backup_dir = strrchr(backup_info_path, '/');
		if (backup_dir != NULL)
		{
			size_t dir_len = backup_dir - backup_info_path;
			snprintf(info->backup_path, sizeof(info->backup_path),
					 "%.*s/%s", (int)dir_len, backup_info_path, info->backup_id);
		}

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

BackupAdapter pgbackrest_adapter = {
	.name = "pgBackRest",
	.detect = pgbackrest_detect,
	.scan = pgbackrest_scan
};

BackupAdapter*
get_pgbackrest_adapter(void)
{
	return &pgbackrest_adapter;
}
