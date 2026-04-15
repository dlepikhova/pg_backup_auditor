/*
 * pg_basebackup.c
 *
 * Adapter for pg_basebackup backups
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

#define _XOPEN_SOURCE 700

#include "pg_backup_auditor.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>

/* Forward declarations */
static bool pg_basebackup_detect(const char *path);
static BackupInfo* pg_basebackup_scan(const char *backup_root);
static int pg_basebackup_read_metadata(const char *backup_path, BackupInfo *info);
static char* pg_basebackup_get_wal_archive_path(const char *backup_path, const char *instance_name);
static void pg_basebackup_cleanup(BackupInfo *info);

/* Helper functions */
static bool is_tar_format(const char *path);
static bool is_plain_format(const char *path);
static int parse_manifest_stream(FILE *fp, BackupInfo *info);
static int parse_backup_manifest(const char *manifest_path, BackupInfo *info);

/* Implemented in src/validator/pg_basebackup_validator.c */
ValidationResult* pg_basebackup_validate_structure(BackupInfo *backup);

/*
 * Shell-free replacement for popen("tar FLAG TARFILE MEMBER", "r").
 * Arguments are passed directly to execvp — no shell, no injection risk.
 * Single-threaded use only (tar_child_pid is a module-level static).
 */
static pid_t tar_child_pid = -1;

static FILE *
tar_popen(const char *flag, const char *tar_path, const char *member)
{
	int    pipefd[2];
	pid_t  pid;
	int    devnull;
	FILE  *fp;

	if (pipe(pipefd) != 0)
		return NULL;

	pid = fork();
	if (pid < 0)
	{
		close(pipefd[0]);
		close(pipefd[1]);
		return NULL;
	}

	if (pid == 0)
	{
		close(pipefd[0]);
		if (dup2(pipefd[1], STDOUT_FILENO) < 0)
			_exit(127);
		close(pipefd[1]);
		devnull = open("/dev/null", O_WRONLY);
		if (devnull >= 0)
		{
			dup2(devnull, STDERR_FILENO);
			close(devnull);
		}
		execlp("tar", "tar", flag, tar_path, member, (char *)NULL);
		_exit(127);
	}

	/* parent */
	close(pipefd[1]);
	fp = fdopen(pipefd[0], "r");
	if (fp == NULL)
	{
		close(pipefd[0]);
		waitpid(pid, NULL, 0);
		return NULL;
	}
	tar_child_pid = pid;
	return fp;
}

static void
tar_pclose(FILE *fp)
{
	fclose(fp);
	if (tar_child_pid > 0)
	{
		waitpid(tar_child_pid, NULL, 0);
		tar_child_pid = -1;
	}
}
WALArchiveInfo*   pg_basebackup_get_embedded_wal(BackupInfo *backup);

/* Adapter definition */
BackupAdapter pg_basebackup_adapter = {
	.name = "pg_basebackup",
	.detect = pg_basebackup_detect,
	.scan = pg_basebackup_scan,
	.read_metadata = pg_basebackup_read_metadata,
	.get_wal_archive_path = pg_basebackup_get_wal_archive_path,
	.validate_structure = pg_basebackup_validate_structure,
	.get_embedded_wal   = pg_basebackup_get_embedded_wal,
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
		log_debug("Failed to parse backup metadata at: %s", backup_path);
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

	/* wal_stream and wal_mode: derived from actual WAL presence, not backup_method.
	 * backup_method="streamed" refers to the data transfer protocol, not WAL handling.
	 * "embedded" = WAL files are inside the backup (--wal-method=stream or fetch).
	 * "none"     = no WAL in backup (--wal-method=none), external archive required. */
	info->wal_stream = (info->wal_bytes > 0);
	snprintf(info->wal_mode, sizeof(info->wal_mode), "%s",
			 info->wal_stream ? "embedded" : "none");

	/* end_time fallback: directory mtime (overridden by manifest mtime below) */
	{
		struct stat st;
		if (stat(backup_path, &st) == 0)
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
			/* Parse major.minor format (e.g., "16.1" -> 160001) */
			int major = 0, minor = 0;
			if (sscanf(version_str, "%d.%d", &major, &minor) >= 1)
			{
				info->pg_version = major * 10000 + minor;
				log_debug("Read PG_VERSION from file: major=%d, minor=%d", major, minor);
			}
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
					const char *flag;

					path_join(tar_file, sizeof(tar_file), backup_path, tar_entry->d_name);

					if (strstr(tar_entry->d_name, ".gz") != NULL)
						flag = "-xzOf";
					else if (strstr(tar_entry->d_name, ".bz2") != NULL)
						flag = "-xjOf";
					else if (strstr(tar_entry->d_name, ".xz") != NULL ||
							 strstr(tar_entry->d_name, ".lz4") != NULL)
						flag = "-xJOf";
					else
						flag = "-xOf";

					ver_fp = tar_popen(flag, tar_file, "PG_VERSION");
					if (ver_fp != NULL)
					{
						char version_str[32];
						if (fgets(version_str, sizeof(version_str), ver_fp) != NULL)
						{
							/* Parse major.minor format (e.g., "16.1" -> 160001) */
							int major = 0, minor = 0;
							if (sscanf(version_str, "%d.%d", &major, &minor) >= 1)
							{
								info->pg_version = major * 10000 + minor;
								log_debug("Extracted PG_VERSION from tar: major=%d, minor=%d", major, minor);
							}
						}
						tar_pclose(ver_fp);
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
	const char *tar_flag = NULL;
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

					/* Determine tar flag based on compression */
					if (strstr(entry->d_name, ".gz") != NULL)
						tar_flag = "-xzOf";
					else if (strstr(entry->d_name, ".bz2") != NULL)
						tar_flag = "-xjOf";
					else if (strstr(entry->d_name, ".xz") != NULL ||
							 strstr(entry->d_name, ".lz4") != NULL)
						tar_flag = "-xJOf";
					else
						tar_flag = "-xOf";

					log_debug("Extracting backup_label from tar: %s", entry->d_name);
					fp = tar_popen(tar_flag, tar_path, "backup_label");
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
		/* CHECKPOINT LOCATION: 0/2000080
		 * This is the LSN of the checkpoint record taken at backup start —
		 * it is NOT the stop_lsn.  The real stop_lsn (End-LSN) is only
		 * available in backup_manifest (PG13+), parsed below.  Storing this
		 * value as stop_lsn would make WAL availability checks too lenient. */
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
		/* INCREMENTAL FROM LSN: 0/6000028 (PostgreSQL 17+)
		 * This LSN equals the stop_lsn of the parent backup.
		 * Stored in redo_lsn; fs_scanner uses it to set parent_backup_id. */
		else if (strncmp(line, "INCREMENTAL FROM LSN:", 21) == 0)
		{
			is_incremental = true;
			value = line + 21;
			while (*value == ' ' || *value == '\t')
				value++;
			unsigned int hi, lo;
			if (sscanf(value, "%X/%X", &hi, &lo) == 2)
				info->redo_lsn = ((uint64_t)hi << 32) | lo;
		}
	}

	/* Close file handle (use tar_pclose for tar, fclose for plain) */
	if (is_tar)
		tar_pclose(fp);
	else
		fclose(fp);

	if (!found_start_time)
	{
		log_debug("START TIME not found in backup_label");
		return STATUS_ERROR;
	}

	/* Update backup type if incremental backup was detected */
	if (is_incremental)
	{
		info->type = BACKUP_TYPE_INCREMENTAL;
		log_debug("Detected incremental backup (PostgreSQL 17+)");

		/*
		 * Append 'I' suffix to avoid backup_id collision when a FULL and an
		 * INCREMENTAL backup happen to start within the same second (rare but
		 * observed in practice with fast test setups).
		 */
		size_t id_len = strlen(info->backup_id);
		if (id_len + 1 < sizeof(info->backup_id))
		{
			info->backup_id[id_len]     = 'I';
			info->backup_id[id_len + 1] = '\0';
		}
	}

	/*
	 * Supplemental read from backup_manifest:
	 * - stop_lsn (End-LSN) is not present in backup_label; backup_manifest
	 *   is the only source of the real stop LSN for pg_basebackup backups.
	 * - For plain format: read directly from the file; also get end_time from mtime.
	 * - For tar format: extract backup_manifest from the same archive via tar_popen.
	 */
	if (!is_tar)
	{
		char manifest_path[PATH_MAX];
		path_join(manifest_path, sizeof(manifest_path),
				  backup_path, "backup_manifest");
		if (file_exists(manifest_path))
			parse_backup_manifest(manifest_path, info);
	}
	else if (tar_flag != NULL && tar_path[0] != '\0')
	{
		FILE *mfp = tar_popen(tar_flag, tar_path, "backup_manifest");
		if (mfp != NULL)
		{
			parse_manifest_stream(mfp, info);
			tar_pclose(mfp);
			log_debug("Extracted backup_manifest from tar: stop_lsn=%llX",
					  (unsigned long long)info->stop_lsn);
		}
		else
			log_debug("backup_manifest not found in tar archive (PG12 or earlier)");
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
 * Get WAL archive path for pg_basebackup backup
 *
 * pg_basebackup can store WAL in two ways:
 * 1. Included in backup (--wal-method=fetch or stream): pg_wal/ inside backup_path
 * 2. External WAL archive: checks standard relative paths (../archive, ../wal_archive, etc.)
 *
 * If found, returns the path (must be freed by caller).
 * If not found, logs advice to use --wal-archive and returns NULL.
 */
static char*
pg_basebackup_get_wal_archive_path(const char *backup_path, const char *instance_name)
{
	char *wal_path;
	char candidate_path[PATH_MAX];
	char resolved_path[PATH_MAX];

	(void) instance_name;  /* Not used for pg_basebackup */

	if (backup_path == NULL)
		return NULL;

	/* Check for pg_wal directory inside backup (embedded WAL) */
	path_join(candidate_path, sizeof(candidate_path), backup_path, "pg_wal");
	if (is_directory(candidate_path))
	{
		wal_path = strdup(candidate_path);
		log_debug("pg_basebackup embedded WAL path: %s", wal_path);
		return wal_path;
	}

	/* Check standard external archive relative paths */
	const char *candidates[] = {
		"../archive",
		"../wal_archive",
		"../pg_wal",
		"../../archive",
		NULL
	};

	for (int i = 0; candidates[i] != NULL; i++)
	{
		path_join(candidate_path, sizeof(candidate_path), backup_path, candidates[i]);

		/* Normalize the path to catch symlinks and relative components */
		if (realpath(candidate_path, resolved_path) != NULL &&
			is_directory(resolved_path))
		{
			wal_path = strdup(resolved_path);
			log_debug("pg_basebackup external WAL archive found: %s (via %s)",
					  wal_path, candidates[i]);
			return wal_path;
		}
	}

	log_debug("No pg_wal/ or external WAL archive found for pg_basebackup at: %s; "
			  "use --wal-archive if external WAL exists", backup_path);
	return NULL;
}

/*
 * parse_lsn_str — parse "X/XXXXXX" into uint64_t.
 * Returns true on success.
 */
static bool
parse_lsn_str(const char *str, uint64_t *out)
{
	unsigned int hi, lo;
	if (sscanf(str, "%X/%X", &hi, &lo) == 2)
	{
		*out = ((uint64_t)hi << 32) | lo;
		return true;
	}
	return false;
}

/*
 * parse_quoted_lsn — find the first quoted string following `key` on `line`
 * and parse it as an LSN.  Returns true on success.
 */
static bool
parse_quoted_lsn(const char *line, const char *key, uint64_t *out)
{
	const char *p = strstr(line, key);
	if (p == NULL)
		return false;
	p += strlen(key);
	p = strchr(p, '"');
	if (p == NULL) return false;
	p++;
	return parse_lsn_str(p, out);
}

/*
 * parse_backup_manifest — read backup_manifest for metadata.
 *
 * Primary use: pg_combinebackup (no backup_label) — extracts Timeline,
 * Start-LSN, and generates a synthetic backup_id/start_time.
 *
 * Supplemental use (called after backup_label is parsed): overrides
 * stop_lsn with the more accurate End-LSN from WAL-Ranges, and sets
 * end_time from the manifest file's mtime (written last by pg_basebackup).
 *
 * The `supplemental` flag controls which fields are updated:
 *   false — full parse, fills start_lsn / timeline / backup_id / start_time
 *   true  — only updates stop_lsn and end_time (leaves other fields alone)
 */
/*
 * parse_manifest_stream — read backup_manifest metadata from an open FILE*.
 * Used by both parse_backup_manifest (plain file) and tar extraction.
 * Does NOT fclose(fp) — caller is responsible.
 * Does NOT set end_time (no path for mtime) — caller sets it if needed.
 */
static int
parse_manifest_stream(FILE *fp, BackupInfo *info)
{
	char   line[2048];
	bool   found_timeline = false;
	bool   found_start_lsn = false;
	bool   found_end_lsn = false;
	time_t now;

	while (fgets(line, sizeof(line), fp) != NULL)
	{
		/* "Timeline": <number> */
		if (!found_timeline && strstr(line, "\"Timeline\"") != NULL)
		{
			const char *p = strstr(line, "\"Timeline\"");
			p = strchr(p, ':');
			if (p != NULL)
			{
				info->timeline = (TimeLineID)atoi(p + 1);
				found_timeline = true;
			}
		}

		/* "Start-LSN": "X/X" — only if not yet set */
		if (!found_start_lsn && strstr(line, "\"Start-LSN\"") != NULL)
		{
			uint64_t lsn = 0;
			if (parse_quoted_lsn(line, "\"Start-LSN\"", &lsn))
			{
				if (info->start_lsn == 0)
					info->start_lsn = lsn;
				found_start_lsn = true;
			}
		}

		/* "End-LSN": "X/X" — always update stop_lsn (more accurate) */
		if (!found_end_lsn && strstr(line, "\"End-LSN\"") != NULL)
		{
			uint64_t lsn = 0;
			if (parse_quoted_lsn(line, "\"End-LSN\"", &lsn))
			{
				info->stop_lsn = lsn;
				found_end_lsn = true;
			}
		}

		/* Stop once we've found all three fields */
		if (found_timeline && found_start_lsn && found_end_lsn)
			break;
	}

	if (!found_timeline && !found_start_lsn)
	{
		log_warning("backup_manifest does not contain Timeline or Start-LSN");
		return STATUS_ERROR;
	}

	/* For pg_combinebackup (no backup_label): generate synthetic backup_id */
	if (info->start_time == 0)
	{
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
		info->start_time = now;
	}

	log_debug("Parsed backup_manifest: timeline=%u, start_lsn=%llX, stop_lsn=%llX",
			  info->timeline,
			  (unsigned long long)info->start_lsn,
			  (unsigned long long)info->stop_lsn);

	return STATUS_OK;
}

static int
parse_backup_manifest(const char *manifest_path, BackupInfo *info)
{
	FILE       *fp;
	int         rc;
	struct stat mst;

	fp = fopen(manifest_path, "r");
	if (fp == NULL)
	{
		log_debug("backup_manifest not found: %s", manifest_path);
		return STATUS_ERROR;
	}

	rc = parse_manifest_stream(fp, info);
	fclose(fp);

	if (rc == STATUS_OK)
	{
		/*
		 * end_time: backup_manifest is the last file written by pg_basebackup,
		 * so its mtime is the best available approximation of backup end time.
		 */
		if (stat(manifest_path, &mst) == 0)
			info->end_time = mst.st_mtime;
	}

	return rc;
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
