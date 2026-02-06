/*
 * cmd_info.c
 *
 * Implementation of 'info' command - show detailed backup information
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


#include "pg_backup_auditor.h"
#include "cmd_help.h"
#include "arg_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>

/* Command-line options */
typedef struct {
	char *backup_path;   /* Direct path to backup directory */
	char *backup_dir;    /* Parent directory to search in */
	char *backup_id;     /* Backup ID to find */
} InfoOptions;

static const char *
get_status_color(BackupStatus status)
{
	if (!use_color)
		return "";

	if (status == BACKUP_STATUS_OK)
		return COLOR_GREEN;
	if (status == BACKUP_STATUS_ERROR || status == BACKUP_STATUS_CORRUPT)
		return COLOR_RED;
	if (status == BACKUP_STATUS_WARNING || status == BACKUP_STATUS_ORPHAN)
		return COLOR_YELLOW;
	if (status == BACKUP_STATUS_RUNNING)
		return COLOR_CYAN;

	return "";
}

static void
init_options(InfoOptions *opts)
{
	opts->backup_path = NULL;
	opts->backup_dir = NULL;
	opts->backup_id = NULL;
}

static int
parse_arguments(int argc, char **argv, InfoOptions *opts)
{
	int c;
	int option_index = 0;
	bool backup_path_seen = false;
	bool backup_dir_seen = false;
	bool backup_id_seen = false;

	static struct option long_options[] = {
		{"backup-path", required_argument, 0, 'p'},
		{"backup-dir",  required_argument, 0, 'B'},
		{"backup-id",   required_argument, 0, 'i'},
		{"help",        no_argument,       0, 'h'},
		{0, 0, 0, 0}
	};

	while ((c = getopt_long(argc, argv, "p:B:i:h",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'p':
				if (check_duplicate_option(backup_path_seen, "--backup-path"))
					return EXIT_INVALID_ARGUMENTS;
				opts->backup_path = optarg;
				backup_path_seen = true;
				break;
			case 'B':
				if (check_duplicate_option(backup_dir_seen, "--backup-dir"))
					return EXIT_INVALID_ARGUMENTS;
				opts->backup_dir = optarg;
				backup_dir_seen = true;
				break;
			case 'i':
				if (check_duplicate_option(backup_id_seen, "--backup-id"))
					return EXIT_INVALID_ARGUMENTS;
				opts->backup_id = optarg;
				backup_id_seen = true;
				break;
			case 'h':
				print_info_usage();
				return EXIT_SUCCESS;
			case '?':
				return EXIT_INVALID_ARGUMENTS;
			default:
				return EXIT_INVALID_ARGUMENTS;
		}
	}

	return -1;  /* Continue processing */
}

static int
validate_options(const InfoOptions *opts)
{
	/* Need either backup-path OR (backup-dir + backup-id) */
	if (opts->backup_path == NULL)
	{
		if (opts->backup_dir == NULL || opts->backup_id == NULL)
		{
			fprintf(stderr, "Error: Either --backup-path or (--backup-dir + --backup-id) is required\n");
			fprintf(stderr, "Try 'pg_backup_auditor info --help' for more information\n");
			return EXIT_INVALID_ARGUMENTS;
		}
	}

	/* Validate paths exist */
	if (opts->backup_path != NULL && !is_directory(opts->backup_path))
	{
		fprintf(stderr, "Error: Backup path does not exist: %s\n", opts->backup_path);
		return EXIT_GENERAL_ERROR;
	}

	if (opts->backup_dir != NULL && !is_directory(opts->backup_dir))
	{
		fprintf(stderr, "Error: Backup directory does not exist: %s\n", opts->backup_dir);
		return EXIT_GENERAL_ERROR;
	}

	return EXIT_SUCCESS;
}

static void
format_duration(time_t start, time_t end, char *buf, size_t bufsize)
{
	if (start == 0 || end == 0 || end < start)
	{
		snprintf(buf, bufsize, "N/A");
		return;
	}

	time_t duration = end - start;
	int hours = duration / 3600;
	int minutes = (duration % 3600) / 60;
	int seconds = duration % 60;

	if (hours > 0)
		snprintf(buf, bufsize, "%dh %dm %ds", hours, minutes, seconds);
	else if (minutes > 0)
		snprintf(buf, bufsize, "%dm %ds", minutes, seconds);
	else
		snprintf(buf, bufsize, "%ds", seconds);
}

static void
print_backup_info(const BackupInfo *backup)
{
	char start_time[64];
	char end_time[64];
	char duration[64];
	char size_str[64];

	printf("====================================================\n");
	printf("Backup Information\n");
	printf("====================================================\n\n");

	/* General Information */
	printf("GENERAL:\n");
	printf("  Backup ID:       %s\n", backup->backup_id);
	printf("  Node:            %s\n", backup->node_name[0] ? backup->node_name : "localhost");
	if (backup->instance_name[0] != '\0')
		printf("  Instance:        %s\n", backup->instance_name);
	printf("  Type:            %s\n", backup_type_to_string(backup->type));
	printf("  Tool:            %s\n", backup_tool_to_string(backup->tool));
	if (backup->tool_version[0] != '\0')
		printf("  Tool Version:    %s\n", backup->tool_version);
	printf("  Status:          %s%s%s\n",
		   get_status_color(backup->status),
		   backup_status_to_string(backup->status),
		   use_color ? COLOR_RESET : "");
	printf("\n");

	/* Timing Information */
	printf("TIMING:\n");
	if (backup->start_time > 0)
		strftime(start_time, sizeof(start_time), "%Y-%m-%d %H:%M:%S",
				 localtime(&backup->start_time));
	else
		snprintf(start_time, sizeof(start_time), "N/A");

	if (backup->end_time > 0)
		strftime(end_time, sizeof(end_time), "%Y-%m-%d %H:%M:%S",
				 localtime(&backup->end_time));
	else
		snprintf(end_time, sizeof(end_time), "N/A");

	format_duration(backup->start_time, backup->end_time, duration, sizeof(duration));

	printf("  Start Time:      %s\n", start_time);
	printf("  End Time:        %s\n", end_time);
	printf("  Duration:        %s\n", duration);
	printf("\n");

	/* Storage Information */
	printf("STORAGE:\n");
	printf("  Path:            %s\n", backup->backup_path);

	if (backup->data_bytes > 0)
	{
		double size_mb = backup->data_bytes / (1024.0 * 1024.0);
		if (size_mb > 1024)
		{
			double size_gb = size_mb / 1024.0;
			if (size_gb > 1024)
				snprintf(size_str, sizeof(size_str), "%.2f TB (%.0f bytes)",
						 size_gb / 1024.0, (double)backup->data_bytes);
			else
				snprintf(size_str, sizeof(size_str), "%.2f GB (%.0f bytes)",
						 size_gb, (double)backup->data_bytes);
		}
		else
			snprintf(size_str, sizeof(size_str), "%.2f MB (%.0f bytes)",
					 size_mb, (double)backup->data_bytes);
	}
	else
		snprintf(size_str, sizeof(size_str), "N/A");

	printf("  Size:            %s\n", size_str);

	/* WAL size if available */
	if (backup->wal_bytes > 0)
	{
		char wal_size_str[64];
		double wal_mb = backup->wal_bytes / (1024.0 * 1024.0);
		if (wal_mb > 1024)
		{
			double wal_gb = wal_mb / 1024.0;
			if (wal_gb > 1024)
				snprintf(wal_size_str, sizeof(wal_size_str), "%.2f TB (%.0f bytes)",
						 wal_gb / 1024.0, (double)backup->wal_bytes);
			else
				snprintf(wal_size_str, sizeof(wal_size_str), "%.2f GB (%.0f bytes)",
						 wal_gb, (double)backup->wal_bytes);
		}
		else
			snprintf(wal_size_str, sizeof(wal_size_str), "%.2f MB (%.0f bytes)",
					 wal_mb, (double)backup->wal_bytes);
		printf("  WAL Size:        %s\n", wal_size_str);
	}

	printf("\n");

	/* PostgreSQL Information */
	printf("POSTGRESQL:\n");
	if (backup->pg_version > 0)
		printf("  PG Version:      %d\n", backup->pg_version / 10000);
	else
		printf("  PG Version:      N/A\n");

	if (backup->timeline > 0)
		printf("  Timeline:        %u\n", backup->timeline);
	else
		printf("  Timeline:        N/A\n");

	if (backup->start_lsn > 0)
		printf("  Start LSN:       %lX/%X\n",
			   (unsigned long)(backup->start_lsn >> 32),
			   (unsigned int)(backup->start_lsn & 0xFFFFFFFF));
	else
		printf("  Start LSN:       N/A\n");

	if (backup->stop_lsn > 0)
		printf("  Stop LSN:        %lX/%X\n",
			   (unsigned long)(backup->stop_lsn >> 32),
			   (unsigned int)(backup->stop_lsn & 0xFFFFFFFF));
	else
		printf("  Stop LSN:        N/A\n");

	/* WAL Range */
	if (backup->start_lsn > 0 && backup->stop_lsn > 0)
	{
		printf("  WAL Range:       %lX/%X -> %lX/%X\n",
			   (unsigned long)(backup->start_lsn >> 32),
			   (unsigned int)(backup->start_lsn & 0xFFFFFFFF),
			   (unsigned long)(backup->stop_lsn >> 32),
			   (unsigned int)(backup->stop_lsn & 0xFFFFFFFF));
	}

	/* Extended metadata */
	if (backup->wal_start_file[0] != '\0')
		printf("  WAL Start File:  %s\n", backup->wal_start_file);

	if (backup->backup_method[0] != '\0')
		printf("  Backup Method:   %s\n", backup->backup_method);

	if (backup->backup_from[0] != '\0')
		printf("  Backup From:     %s\n", backup->backup_from);

	if (backup->backup_label[0] != '\0')
		printf("  Label:           %s\n", backup->backup_label);

	printf("\n");

	printf("====================================================\n");
}

static BackupInfo*
find_backup_by_id(const char *backup_dir, const char *backup_id)
{
	BackupInfo *all_backups;
	BackupInfo *current;
	BackupInfo *found = NULL;

	/* Scan all backups (unlimited depth) */
	all_backups = scan_backup_directory(backup_dir, -1);
	if (all_backups == NULL)
		return NULL;

	/* Find backup with matching ID */
	current = all_backups;
	while (current != NULL)
	{
		if (strcmp(current->backup_id, backup_id) == 0)
		{
			/* Allocate and copy the found backup */
			found = malloc(sizeof(BackupInfo));
			if (found != NULL)
			{
				memcpy(found, current, sizeof(BackupInfo));
				found->next = NULL;
			}
			break;
		}
		current = current->next;
	}

	/* Free the list */
	free_backup_list(all_backups);

	return found;
}

/*
 * cmd_info_main - Main function for the 'info' command
 *
 * Displays detailed information about a specific backup in human-readable format.
 * Supports two modes of backup access:
 *
 * Mode 1 (--backup-path): Direct access by backup path
 *   - Detects backup type via detect_backup_type()
 *   - Extracts metadata via corresponding adapter
 *
 * Mode 2 (--backup-dir + --backup-id): Search by ID
 *   - Scans entire backup directory
 *   - Finds backup with specified backup_id
 *
 * Outputs information in 4 sections:
 * - GENERAL: ID, node, type, tool, status
 * - TIMING: start time, end time, duration
 * - STORAGE: size, path, compression
 * - POSTGRESQL: version, timeline, LSN range, WAL metadata
 *
 * Returns:
 * - EXIT_SUCCESS (0) on successful information display
 * - EXIT_FAILURE (1) if backup not found or read error
 * - 4 on invalid arguments
 */
int
cmd_info_main(int argc, char **argv)
{
	InfoOptions opts;
	BackupInfo *backup = NULL;
	int ret;

	/* Initialize logging */
	log_init();

	/* Initialize options */
	init_options(&opts);

	/* Parse command-line arguments */
	ret = parse_arguments(argc, argv, &opts);
	if (ret == EXIT_SUCCESS)  /* --help was shown */
		return EXIT_SUCCESS;
	if (ret != -1)  /* Error occurred */
		return ret;

	/* Validate options */
	ret = validate_options(&opts);
	if (ret != EXIT_SUCCESS)
		return ret;

	/* Get backup information */
	if (opts.backup_path != NULL)
	{
		/* Scan specific backup directory (depth 0 - only that directory) */
		log_info("Loading backup from: %s", opts.backup_path);
		backup = scan_backup_directory(opts.backup_path, 0);
	}
	else
	{
		/* Find backup by ID */
		log_info("Searching for backup ID '%s' in: %s", opts.backup_id, opts.backup_dir);
		backup = find_backup_by_id(opts.backup_dir, opts.backup_id);
	}

	if (backup == NULL)
	{
		if (opts.backup_id != NULL)
			fprintf(stderr, "Error: Backup with ID '%s' not found\n", opts.backup_id);
		else
			fprintf(stderr, "Error: No backup found at: %s\n", opts.backup_path);
		return EXIT_NO_BACKUPS_FOUND;
	}

	/* Display backup information */
	print_backup_info(backup);

	/* Cleanup */
	if (opts.backup_path == NULL)
		free(backup);  /* We allocated this for find_backup_by_id */
	else
		free_backup_list(backup);

	return EXIT_SUCCESS;
}
