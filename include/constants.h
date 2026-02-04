/*
 * constants.h
 *
 * Constants for pg_backup_auditor
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


#ifndef CONSTANTS_H
#define CONSTANTS_H

/* Exit codes - General */
#define EXIT_SUCCESS            0
#define EXIT_GENERAL_ERROR      1
#define EXIT_INVALID_ARGUMENTS  4

/* Exit codes - check command specific */
#define EXIT_VALIDATION_FAILED  2
#define EXIT_CRITICAL_ERROR     3

/* Exit codes - list command specific */
#define EXIT_NO_BACKUPS_FOUND   3

/* File names */
#define BACKUP_LABEL_FILE       "backup_label"
#define BACKUP_CONTROL_FILE     "backup.control"
#define TABLESPACE_MAP_FILE     "tablespace_map"
#define PG_VERSION_FILE         "PG_VERSION"

/* Directory names */
#define BASE_DIR                "base"
#define GLOBAL_DIR              "global"
#define PG_WAL_DIR              "pg_wal"
#define DATABASE_DIR            "database"
#define BACKUPS_DIR             "backups"

/* WAL file name length */
#define WAL_SEGMENT_NAME_LENGTH 24

/* Default values */
#define DEFAULT_RETENTION_DAYS   7
#define DEFAULT_RETENTION_WEEKLY 4
#define DEFAULT_THREADS          1

#endif /* CONSTANTS_H */
