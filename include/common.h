/*
 * common.h
 *
 * Common utility functions
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


#ifndef COMMON_H
#define COMMON_H

#include "types.h"
#include <stdbool.h>
#include <stddef.h>
#include <sys/types.h>

/* xlog.c - LSN and WAL utilities */
bool parse_lsn(const char *str, XLogRecPtr *lsn);
int lsn_compare(XLogRecPtr lsn1, XLogRecPtr lsn2);
void format_lsn(XLogRecPtr lsn, char *buf, size_t bufsize);
void lsn_to_seg(XLogRecPtr lsn, uint32_t timeline, WALSegmentName *seg, uint32_t wal_segment_size);
bool parse_wal_filename(const char *filename, WALSegmentName *result);

/* logging.c - Logging functions */
typedef enum {
	LOG_DEBUG,
	LOG_INFO,
	LOG_WARNING,
	LOG_ERROR
} LogLevel;

void log_init(void);
void log_set_level(LogLevel level);
void log_set_file(const char *filename);
void log_debug(const char *fmt, ...);
void log_info(const char *fmt, ...);
void log_warning(const char *fmt, ...);
void log_error(const char *fmt, ...);
void log_cleanup(void);

/* Color support */
extern bool use_color;

#define COLOR_RED     "\033[0;31m"
#define COLOR_GREEN   "\033[0;32m"
#define COLOR_YELLOW  "\033[0;33m"
#define COLOR_BLUE    "\033[0;34m"
#define COLOR_CYAN    "\033[0;36m"
#define COLOR_BOLD    "\033[1m"
#define COLOR_RESET   "\033[0m"

/* string_utils.c - String utilities */
char *str_trim(char *str);
char **str_split(char *str, char delimiter, int *count);
void str_copy(char *dest, const char *src, size_t destsize);

/* file_utils.c - File system utilities */
bool file_exists(const char *path);
bool is_directory(const char *path);
bool is_regular_file(const char *path);
off_t get_file_size(const char *path);
uint64_t get_directory_size(const char *path);
void path_join(char *dest, size_t destsize, const char *path1, const char *path2);
char *read_file_contents(const char *path);

/* scanner/fs_scanner.c - Directory scanning */
BackupInfo* scan_backup_directory(const char *backup_dir, int max_depth);
WALArchiveInfo* scan_wal_archive(const char *wal_archive_dir);
void free_backup_list(BackupInfo *list);
void free_wal_archive_info(WALArchiveInfo *info);

/* validator/backup_validator.c - Backup validation */
ValidationResult* validate_backup_metadata(BackupInfo *info);
ValidationResult* validate_backup_chain(BackupInfo *backup, BackupInfo *all_backups);
ValidationResult* check_retention_policy(BackupInfo *backups, int retention_days, int retention_weekly);
void free_validation_result(ValidationResult *result);

/* validator/wal_validator.c - WAL validation */
ValidationResult* check_wal_continuity(WALArchiveInfo *wal_info);
ValidationResult* check_wal_availability(BackupInfo *backup, WALArchiveInfo *wal_info);

#endif /* COMMON_H */
