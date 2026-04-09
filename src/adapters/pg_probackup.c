/*
 * pg_probackup.c
 *
 * Adapter for pg_probackup backups
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

/* Forward declarations */
static bool pg_probackup_detect(const char *path);
static BackupInfo* pg_probackup_scan(const char *backup_root);
static int pg_probackup_read_metadata(const char *backup_path, BackupInfo *info);
static char* pg_probackup_get_wal_archive_path(const char *backup_path, const char *instance_name);
static void pg_probackup_cleanup(BackupInfo *info);

/* Implemented in src/validator/pg_probackup_validator.c */
ValidationResult* pg_probackup_validate_structure(BackupInfo *backup);
WALArchiveInfo*   pg_probackup_get_embedded_wal(BackupInfo *backup);

/* Adapter definition */
BackupAdapter pg_probackup_adapter = {
	.name = "pg_probackup",
	.detect = pg_probackup_detect,
	.scan = pg_probackup_scan,
	.read_metadata = pg_probackup_read_metadata,
	.get_wal_archive_path = pg_probackup_get_wal_archive_path,
	.validate_structure = pg_probackup_validate_structure,
	.get_embedded_wal   = pg_probackup_get_embedded_wal,
	.cleanup = pg_probackup_cleanup
};

/*
 * Detect if path contains a pg_probackup backup
 *
 * pg_probackup 2.5.X structure:
 * - backup.control file (required)
 * - database/ directory (required for data)
 */
static bool
pg_probackup_detect(const char *path)
{
	char file_path[PATH_MAX];
	bool has_control = false;
	bool has_database = false;

	/* First check if path exists and is a directory */
	if (!is_directory(path))
		return false;

	/* Check for backup.control file */
	path_join(file_path, sizeof(file_path), path, "backup.control");
	has_control = file_exists(file_path);

	/* Check for database/ directory */
	path_join(file_path, sizeof(file_path), path, "database");
	has_database = is_directory(file_path);

	/* Valid pg_probackup backup needs both */
	if (has_control && has_database)
	{
		log_debug("Detected pg_probackup 2.5.X format at: %s", path);
		return true;
	}

	return false;
}

/*
 * Scan a single pg_probackup backup directory
 *
 * Note: fs_scanner.c already handles directory traversal,
 * so this function processes ONE backup directory, not a tree of backups.
 */
static BackupInfo*
pg_probackup_scan(const char *backup_path)
{
	BackupInfo *info;

	log_debug("Scanning pg_probackup backup: %s", backup_path);

	/* Allocate BackupInfo structure */
	info = (BackupInfo *) malloc(sizeof(BackupInfo));
	if (info == NULL)
	{
		log_error("Failed to allocate memory for BackupInfo");
		return NULL;
	}

	/* Initialize the structure */
	memset(info, 0, sizeof(BackupInfo));
	info->next = NULL;

	/* Read metadata from backup.control */
	if (pg_probackup_read_metadata(backup_path, info) != STATUS_OK)
	{
		log_error("Failed to read pg_probackup metadata from: %s", backup_path);
		free(info);
		return NULL;
	}

	/* If backup_id is empty, try to extract from directory name */
	if (info->backup_id[0] == '\0')
	{
		const char *dir_name = strrchr(backup_path, '/');
		if (dir_name != NULL)
		{
			dir_name++;  /* Skip the '/' */
			strncpy(info->backup_id, dir_name, sizeof(info->backup_id) - 1);
			info->backup_id[sizeof(info->backup_id) - 1] = '\0';
		}
	}

	/*
	 * Extract instance name from path
	 * pg_probackup structure: /path/to/backups/INSTANCE_NAME/BACKUP_ID/
	 * We need to get the parent directory name (INSTANCE_NAME)
	 */
	{
		const char *backup_id_pos = strrchr(backup_path, '/');
		if (backup_id_pos != NULL && backup_id_pos != backup_path)
		{
			/* Find the instance name (parent of backup_id) */
			const char *instance_end = backup_id_pos;
			const char *instance_start = backup_id_pos - 1;

			/* Go back to find the previous slash */
			while (instance_start > backup_path && *instance_start != '/')
				instance_start--;

			if (*instance_start == '/')
			{
				instance_start++;  /* Skip the '/' */
				size_t len = instance_end - instance_start;
				if (len > 0 && len < sizeof(info->instance_name))
				{
					memcpy(info->instance_name, instance_start, len);
					info->instance_name[len] = '\0';
					log_debug("Extracted instance name: %s", info->instance_name);
				}
			}
		}
	}

	log_debug("Found pg_probackup backup: %s (instance=%s, type=%d, status=%d)",
			  info->backup_id, info->instance_name, info->type, info->status);

	return info;
}

/*
 * Parse timestamp from pg_probackup format
 * Expected formats:
 * - 'YYYY-MM-DD HH:MM:SS' (with quotes)
 * - YYYY-MM-DD HH:MM:SS+TZ
 */
static time_t
parse_pg_probackup_timestamp(const char *str)
{
	struct tm tm_time = {0};
	int year, mon, mday, hour, min, sec;

	/* Try to parse: YYYY-MM-DD HH:MM:SS */
	if (sscanf(str, "%d-%d-%d %d:%d:%d",
			   &year, &mon, &mday, &hour, &min, &sec) == 6)
	{
		tm_time.tm_year = year - 1900;  /* years since 1900 */
		tm_time.tm_mon = mon - 1;       /* 0-11 */
		tm_time.tm_mday = mday;
		tm_time.tm_hour = hour;
		tm_time.tm_min = min;
		tm_time.tm_sec = sec;
		tm_time.tm_isdst = -1;           /* auto-detect DST */

		return mktime(&tm_time);
	}

	return 0;
}

/*
 * Parse a single line from backup.control file
 * Format: key = value
 */
static void
parse_control_line(const char *line, const char *key, char *value, size_t value_size)
{
	char *equals;
	char *start;
	char *end;

	/* Find the equals sign */
	equals = strchr(line, '=');
	if (equals == NULL)
		return;

	/* Check if this is the key we're looking for */
	if (strncmp(line, key, strlen(key)) != 0)
		return;

	/* Skip whitespace after equals */
	start = equals + 1;
	while (*start == ' ' || *start == '\t')
		start++;

	/* Remove quotes if present */
	if (*start == '\'')
	{
		start++;
		end = strchr(start, '\'');
		if (end != NULL)
		{
			size_t len = end - start;
			if (len >= value_size)
				len = value_size - 1;
			memcpy(value, start, len);
			value[len] = '\0';
			return;
		}
	}

	/* Copy value, removing trailing newline/whitespace */
	end = start + strlen(start) - 1;
	while (end > start && (*end == '\n' || *end == '\r' || *end == ' ' || *end == '\t'))
		end--;

	size_t len = end - start + 1;
	if (len >= value_size)
		len = value_size - 1;
	memcpy(value, start, len);
	value[len] = '\0';
}

/*
 * Read metadata from backup.control file
 *
 * Parse backup.control file (key = value format):
 * - backup-mode = FULL|PAGE|DELTA|PTRACK
 * - status = OK|RUNNING|CORRUPT|ERROR|ORPHAN
 * - backup-id = <timestamp>
 * - start-lsn = <lsn>
 * - stop-lsn = <lsn>
 * - start-time = '<timestamp>'
 * - end-time = '<timestamp>'
 * - timeline = <int>
 * - parent-backup-id = <timestamp>  (for incremental)
 * - data-bytes = <int>
 * - wal-bytes = <int>
 * - compress-alg = <string>  (none|zlib|pglz|lz4|zstd)
 * - from-replica = <bool>   (true = standby, false = primary)
 * - postgres-version = <string>
 */
static int
pg_probackup_read_metadata(const char *backup_path, BackupInfo *info)
{
	char control_path[PATH_MAX];
	FILE *fp;
	char line[1024];
	char value[256] = {0};

	/* Construct path to backup.control */
	path_join(control_path, sizeof(control_path), backup_path, "backup.control");

	/* Open backup.control file */
	fp = fopen(control_path, "r");
	if (fp == NULL)
	{
		log_error("Failed to open backup.control: %s", control_path);
		return STATUS_ERROR;
	}

	/* Parse each line */
	while (fgets(line, sizeof(line), fp) != NULL)
	{
		/* Parse backup-mode */
		parse_control_line(line, "backup-mode", value, sizeof(value));
		if (value[0] != '\0')
		{
			if (strcmp(value, "FULL") == 0)
				info->type = BACKUP_TYPE_FULL;
			else if (strcmp(value, "PAGE") == 0)
				info->type = BACKUP_TYPE_PAGE;
			else if (strcmp(value, "DELTA") == 0)
				info->type = BACKUP_TYPE_DELTA;
			else if (strcmp(value, "PTRACK") == 0)
				info->type = BACKUP_TYPE_PTRACK;
			value[0] = '\0';
			continue;
		}

		/* Parse status */
		parse_control_line(line, "status", value, sizeof(value));
		if (value[0] != '\0')
		{
			if (strcmp(value, "OK") == 0)
				info->status = BACKUP_STATUS_OK;
			else if (strcmp(value, "RUNNING") == 0)
				info->status = BACKUP_STATUS_RUNNING;
			else if (strcmp(value, "CORRUPT") == 0)
				info->status = BACKUP_STATUS_CORRUPT;
			else if (strcmp(value, "ERROR") == 0)
				info->status = BACKUP_STATUS_ERROR;
			else if (strcmp(value, "ORPHAN") == 0)
				info->status = BACKUP_STATUS_ORPHAN;
			value[0] = '\0';
			continue;
		}

		/* Parse backup-id */
		parse_control_line(line, "backup-id", value, sizeof(value));
		if (value[0] != '\0')
		{
			strncpy(info->backup_id, value, sizeof(info->backup_id) - 1);
			info->backup_id[sizeof(info->backup_id) - 1] = '\0';
			value[0] = '\0';
			continue;
		}

		/* Parse start-lsn */
		parse_control_line(line, "start-lsn", value, sizeof(value));
		if (value[0] != '\0')
		{
			parse_lsn(value, &info->start_lsn);
			value[0] = '\0';
			continue;
		}

		/* Parse stop-lsn */
		parse_control_line(line, "stop-lsn", value, sizeof(value));
		if (value[0] != '\0')
		{
			parse_lsn(value, &info->stop_lsn);
			value[0] = '\0';
			continue;
		}

		/* Parse start-time */
		parse_control_line(line, "start-time", value, sizeof(value));
		if (value[0] != '\0')
		{
			info->start_time = parse_pg_probackup_timestamp(value);
			value[0] = '\0';
			continue;
		}

		/* Parse end-time */
		parse_control_line(line, "end-time", value, sizeof(value));
		if (value[0] != '\0')
		{
			info->end_time = parse_pg_probackup_timestamp(value);
			value[0] = '\0';
			continue;
		}

		/* Parse timeline (can be either 'timeline' or 'timelineid') */
		parse_control_line(line, "timelineid", value, sizeof(value));
		if (value[0] == '\0')
			parse_control_line(line, "timeline", value, sizeof(value));
		if (value[0] != '\0')
		{
			info->timeline = (TimeLineID) atoi(value);
			value[0] = '\0';
			continue;
		}

		/* Parse parent-backup-id */
		parse_control_line(line, "parent-backup-id", value, sizeof(value));
		if (value[0] != '\0')
		{
			strncpy(info->parent_backup_id, value, sizeof(info->parent_backup_id) - 1);
			info->parent_backup_id[sizeof(info->parent_backup_id) - 1] = '\0';
			value[0] = '\0';
			continue;
		}

		/* Parse data-bytes */
		parse_control_line(line, "data-bytes", value, sizeof(value));
		if (value[0] != '\0')
		{
			info->data_bytes = (uint64_t) strtoull(value, NULL, 10);
			value[0] = '\0';
			continue;
		}

		/* Parse wal-bytes */
		parse_control_line(line, "wal-bytes", value, sizeof(value));
		if (value[0] != '\0')
		{
			info->wal_bytes = (uint64_t) strtoull(value, NULL, 10);
			value[0] = '\0';
			continue;
		}

		/* Parse server-version (PostgreSQL version) */
		parse_control_line(line, "server-version", value, sizeof(value));
		if (value[0] != '\0')
		{
			info->pg_version = (uint32_t)(atoi(value) * 10000);
			value[0] = '\0';
			continue;
		}

		/* Parse program-version (pg_probackup version) */
		parse_control_line(line, "program-version", value, sizeof(value));
		if (value[0] != '\0')
		{
			strncpy(info->tool_version, value, sizeof(info->tool_version) - 1);
			info->tool_version[sizeof(info->tool_version) - 1] = '\0';
			value[0] = '\0';
			continue;
		}

		/* Parse stream (WAL mode: stream=true means WAL embedded, false means external archive) */
		parse_control_line(line, "stream", value, sizeof(value));
		if (value[0] != '\0')
		{
			info->wal_stream = (strcmp(value, "true") == 0);
			snprintf(info->wal_mode, sizeof(info->wal_mode), "%s",
					 info->wal_stream ? "stream" : "archive");
			value[0] = '\0';
			continue;
		}

		/* Parse from-replica (true = standby, false = primary) */
		parse_control_line(line, "from-replica", value, sizeof(value));
		if (value[0] != '\0')
		{
			snprintf(info->backup_from, sizeof(info->backup_from), "%s",
					 strcmp(value, "true") == 0 ? "standby" : "primary");
			value[0] = '\0';
			continue;
		}

		/* Parse compress-alg (skip "none" — means no compression) */
		parse_control_line(line, "compress-alg", value, sizeof(value));
		if (value[0] != '\0')
		{
			if (strcmp(value, "none") != 0)
				snprintf(info->compress_alg, sizeof(info->compress_alg), "%s", value);
			value[0] = '\0';
			continue;
		}
	}

	fclose(fp);

	/*
	 * Try to read redo_lsn from database/backup_label.
	 *
	 * PostgreSQL writes backup_label into the data directory at backup start.
	 * START WAL LOCATION = the redo pointer of the forced checkpoint = the
	 * oldest WAL location needed for recovery.
	 *
	 * This value may differ from start-lsn in backup.control if pg_probackup
	 * stored the checkpoint record LSN (CHECKPOINT LOCATION) there instead.
	 * When they fall in different WAL segments, the redo segment must also be
	 * present in the archive for recovery to succeed.
	 */
	{
		char   bl_path[PATH_MAX];
		FILE  *blf;
		char   bl_line[256];

		path_join(bl_path, sizeof(bl_path), backup_path, "database");
		path_join(bl_path, sizeof(bl_path), bl_path, "backup_label");

		blf = fopen(bl_path, "r");
		if (blf != NULL)
		{
			while (fgets(bl_line, sizeof(bl_line), blf) != NULL)
			{
				if (strncmp(bl_line, "START WAL LOCATION:", 19) == 0)
				{
					unsigned int hi, lo;
					if (sscanf(bl_line + 19, " %X/%X", &hi, &lo) == 2)
						info->redo_lsn = ((uint64_t)hi << 32) | lo;
					break;
				}
			}
			fclose(blf);
			if (info->redo_lsn != 0)
				log_debug("Parsed redo_lsn from backup_label: %X/%X",
						  (uint32_t)(info->redo_lsn >> 32),
						  (uint32_t) info->redo_lsn);
		}
	}

	/* Set tool type */
	info->tool = BACKUP_TOOL_PG_PROBACKUP;

	/* Copy backup path */
	strncpy(info->backup_path, backup_path, sizeof(info->backup_path) - 1);
	info->backup_path[sizeof(info->backup_path) - 1] = '\0';

	log_debug("Parsed pg_probackup metadata: backup_id=%s, type=%d, status=%d",
			  info->backup_id, info->type, info->status);

	return STATUS_OK;
}

/*
 * Get WAL archive path for pg_probackup backup
 *
 * pg_probackup can store WAL in two places:
 * 1. Default: <backup_catalog>/wal/<instance_name>/
 * 2. Custom: User-specified directory (configured in pg_probackup.conf or via --archive-host)
 *
 * The backup_path typically looks like: <backup_catalog>/backups/<instance_name>/<backup_id>
 *
 * TODO: Read custom WAL location from pg_probackup.conf if it exists
 * For now, we use the default location.
 */
static char*
pg_probackup_get_wal_archive_path(const char *backup_path, const char *instance_name)
{
	char *wal_path;
	char catalog_path[PATH_MAX];
	char config_path[PATH_MAX];
	const char *ptr;
	int slash_count;

	if (backup_path == NULL)
		return NULL;

	/* If instance_name is provided, use it to build the path */
	if (instance_name != NULL && instance_name[0] != '\0')
	{
		/* Navigate from backup_path to catalog root
		 * backup_path: /path/to/catalog/backups/instance_name/backup_id
		 * We need:      /path/to/catalog/wal/instance_name/ */

		/* Find catalog root by going up from backup_path
		 * Remove: /backups/instance_name/backup_id */
		slash_count = 0;
		ptr = backup_path + strlen(backup_path) - 1;

		/* Skip trailing slashes */
		while (ptr > backup_path && *ptr == '/')
			ptr--;

		/* Go up 3 directory levels */
		while (ptr > backup_path && slash_count < 3)
		{
			if (*ptr == '/')
				slash_count++;
			ptr--;
		}

		if (slash_count == 3 && ptr > backup_path)
		{
			/* Copy catalog path */
			size_t len = (ptr + 2) - backup_path;  /* +2 to include the slash */
			if (len >= sizeof(catalog_path))
				len = sizeof(catalog_path) - 1;
			memcpy(catalog_path, backup_path, len);
			catalog_path[len] = '\0';

			/* Check if pg_probackup.conf exists and read custom WAL path
			 * TODO: Implement pg_probackup.conf parsing
			 * Config location: <catalog_path>/pg_probackup.conf */
			path_join(config_path, sizeof(config_path), catalog_path, "pg_probackup.conf");
			if (file_exists(config_path))
			{
				log_debug("Found pg_probackup.conf, but custom WAL path parsing not yet implemented");
				/* Future: parse config and check for archive-host or custom wal directory */
			}

			/* Use default WAL location */
			wal_path = malloc(PATH_MAX);
			if (wal_path == NULL)
				return NULL;

			path_join(wal_path, PATH_MAX, catalog_path, "wal");
			path_join(wal_path, PATH_MAX, wal_path, instance_name);

			/* Verify the path exists */
			if (is_directory(wal_path))
			{
				log_debug("pg_probackup WAL path: %s", wal_path);
				return wal_path;
			}
			else
			{
				log_warning("WAL directory not found: %s", wal_path);
				free(wal_path);
				return NULL;
			}
		}
	}

	/* Fallback: could not determine WAL path */
	log_warning("Could not determine WAL archive path for pg_probackup backup: %s", backup_path);
	log_warning("Instance name: %s", instance_name ? instance_name : "(null)");
	return NULL;
}


/*
 * Cleanup resources
 */
static void
pg_probackup_cleanup(BackupInfo *info)
{
	/* TODO: Free any allocated resources */
	(void) info;  /* unused for now */
}
