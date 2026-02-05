/*
 * types.h
 *
 * Type definitions for pg_backup_auditor
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


#ifndef TYPES_H
#define TYPES_H

#include <stdint.h>
#include <time.h>
#include <limits.h>

/* PATH_MAX fallback for systems where it's not defined */
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

/* PostgreSQL types */
typedef uint64_t XLogRecPtr;
typedef uint32_t TimeLineID;

/* Backup types */
typedef enum {
	BACKUP_TYPE_FULL,
	BACKUP_TYPE_INCREMENTAL,  /* pg_basebackup 17+ incremental */
	BACKUP_TYPE_PAGE,
	BACKUP_TYPE_DELTA,
	BACKUP_TYPE_PTRACK
} BackupType;

/* Backup tools */
typedef enum {
	BACKUP_TOOL_UNKNOWN,
	BACKUP_TOOL_PG_BASEBACKUP,
	BACKUP_TOOL_PG_PROBACKUP
} BackupTool;

/* Backup status */
typedef enum {
	BACKUP_STATUS_OK,
	BACKUP_STATUS_RUNNING,
	BACKUP_STATUS_CORRUPT,
	BACKUP_STATUS_ERROR,
	BACKUP_STATUS_ORPHAN,
	BACKUP_STATUS_WARNING
} BackupStatus;

/* Validation level */
typedef enum {
	VALIDATION_LEVEL_BASIC,      /* Level 1: File structure + chain + WAL presence */
	VALIDATION_LEVEL_STANDARD,   /* Level 2: + metadata validation (default) */
	VALIDATION_LEVEL_CHECKSUMS,  /* Level 3: + WAL continuity + checksums */
	VALIDATION_LEVEL_FULL        /* Level 4: All possible checks */
} ValidationLevel;

/* Backup information structure */
typedef struct BackupInfo {
	char            backup_id[64];
	char            node_name[64];      /* Node/host identifier */
	char            instance_name[64];  /* pg_probackup instance name */
	BackupType      type;
	BackupTool      tool;
	BackupStatus    status;
	time_t          start_time;
	time_t          end_time;
	XLogRecPtr      start_lsn;
	XLogRecPtr      stop_lsn;
	TimeLineID      timeline;
	uint32_t        pg_version;
	char            tool_version[32];   /* pg_probackup/pg_basebackup version */
	char            parent_backup_id[64];
	char            backup_path[PATH_MAX];
	uint64_t        data_bytes;
	uint64_t        wal_bytes;
	/* Extended metadata from backup_label */
	char            backup_method[32];  /* "streamed" or "fetch" */
	char            backup_from[32];    /* "primary" or "standby" */
	char            backup_label[128];  /* User-defined label */
	char            wal_start_file[64]; /* WAL filename from START WAL LOCATION */
	struct BackupInfo *next;
} BackupInfo;

/* WAL segment information */
typedef struct {
	uint32_t timeline;
	uint32_t log_id;
	uint32_t seg_id;
} WALSegmentName;

/* WAL archive information */
typedef struct {
	char            archive_path[PATH_MAX];
	int             segment_count;
	WALSegmentName *segments;
} WALArchiveInfo;

/* Validation result */
typedef struct {
	BackupStatus    status;
	int             error_count;
	int             warning_count;
	char          **errors;
	char          **warnings;
} ValidationResult;

/* Generic status codes */
typedef enum {
	STATUS_OK = 0,
	STATUS_ERROR = -1,
	STATUS_WARNING = 1
} Status;

#endif /* TYPES_H */
