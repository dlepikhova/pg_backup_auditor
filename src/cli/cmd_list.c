/*
 * cmd_list.c
 *
 * Implementation of 'list' command
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
#include "cmd_help.h"
#include "arg_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <getopt.h>

/* Command-line options */
typedef struct {
	char *backup_dir;
	char *type_filter;      /* auto|pg_basebackup|pg_probackup */
	char *status_filter;    /* all|ok|warning|error|corrupt|orphan */
	char *format;           /* table|json|yaml */
	char *sort_by;          /* time|lsn|size|status */
	bool reverse;
	int limit;
	int max_depth;          /* Maximum recursion depth (-1 = unlimited) */
} ListOptions;

/* Output statistics */
typedef struct {
	int count;
	uint64_t total_bytes;
} OutputStats;


static void
init_options(ListOptions *opts)
{
	opts->backup_dir = NULL;
	opts->type_filter = "auto";
	opts->status_filter = "all";
	opts->format = "table";
	opts->sort_by = "time";
	opts->reverse = false;
	opts->limit = 0;  /* 0 means no limit */
	opts->max_depth = -1;  /* -1 means unlimited */
}

static int
parse_arguments(int argc, char **argv, ListOptions *opts)
{
	int c;
	int option_index = 0;
	bool backup_dir_seen = false;
	bool type_seen = false;
	bool status_seen = false;
	bool format_seen = false;
	bool sort_by_seen = false;
	bool reverse_seen = false;
	bool limit_seen = false;
	bool max_depth_seen = false;

	static struct option long_options[] = {
		{"backup-dir",  required_argument, 0, 'B'},
		{"type",        required_argument, 0, 't'},
		{"status",      required_argument, 0, 's'},
		{"format",      required_argument, 0, 'f'},
		{"sort-by",     required_argument, 0,  0 },
		{"reverse",     no_argument,       0, 'r'},
		{"limit",       required_argument, 0, 'n'},
		{"max-depth",   required_argument, 0, 'd'},
		{"help",        no_argument,       0, 'h'},
		{0, 0, 0, 0}
	};

	while ((c = getopt_long(argc, argv, "B:t:s:f:rn:d:h",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'B':
				if (check_duplicate_option(backup_dir_seen, "--backup-dir"))
					return EXIT_INVALID_ARGUMENTS;
				opts->backup_dir = optarg;
				backup_dir_seen = true;
				break;
			case 't':
				if (check_duplicate_option(type_seen, "--type"))
					return EXIT_INVALID_ARGUMENTS;
				opts->type_filter = optarg;
				type_seen = true;
				break;
			case 's':
				if (check_duplicate_option(status_seen, "--status"))
					return EXIT_INVALID_ARGUMENTS;
				opts->status_filter = optarg;
				status_seen = true;
				break;
			case 'f':
				if (check_duplicate_option(format_seen, "--format"))
					return EXIT_INVALID_ARGUMENTS;
				opts->format = optarg;
				format_seen = true;
				break;
			case 0:
				/* Long option without short equivalent (--sort-by) */
				if (strcmp(long_options[option_index].name, "sort-by") == 0)
				{
					if (check_duplicate_option(sort_by_seen, "--sort-by"))
						return EXIT_INVALID_ARGUMENTS;
					opts->sort_by = optarg;
					sort_by_seen = true;
				}
				break;
			case 'r':
				if (check_duplicate_option(reverse_seen, "--reverse"))
					return EXIT_INVALID_ARGUMENTS;
				opts->reverse = true;
				reverse_seen = true;
				break;
			case 'n':
				if (check_duplicate_option(limit_seen, "--limit"))
					return EXIT_INVALID_ARGUMENTS;
				if (!parse_int_argument(optarg, &opts->limit, "--limit"))
					return EXIT_INVALID_ARGUMENTS;
				if (opts->limit < 0)
				{
					fprintf(stderr, "Error: --limit must be >= 0\n");
					return EXIT_INVALID_ARGUMENTS;
				}
				limit_seen = true;
				break;
			case 'd':
				if (check_duplicate_option(max_depth_seen, "--max-depth"))
					return EXIT_INVALID_ARGUMENTS;
				if (!parse_int_argument(optarg, &opts->max_depth, "--max-depth"))
					return EXIT_INVALID_ARGUMENTS;
				if (opts->max_depth < -1)
				{
					fprintf(stderr, "Error: --max-depth must be >= -1\n");
					return EXIT_INVALID_ARGUMENTS;
				}
				max_depth_seen = true;
				break;
			case 'h':
				print_list_usage();
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
validate_options(const ListOptions *opts)
{
	/* Check required options */
	if (opts->backup_dir == NULL)
	{
		fprintf(stderr, "Error: --backup-dir is required\n");
		fprintf(stderr, "Try 'pg_backup_auditor list --help' for more information\n");
		return EXIT_INVALID_ARGUMENTS;
	}

	/* Check if backup directory exists */
	if (!is_directory(opts->backup_dir))
	{
		fprintf(stderr, "Error: Backup directory does not exist or is not a directory: %s\n",
				opts->backup_dir);
		return EXIT_GENERAL_ERROR;
	}

	/* Validate type filter */
	if (strcasecmp(opts->type_filter, "auto") != 0 &&
		strcasecmp(opts->type_filter, "pg_basebackup") != 0 &&
		strcasecmp(opts->type_filter, "pg_probackup") != 0)
	{
		fprintf(stderr, "Error: Invalid type filter: %s\n", opts->type_filter);
		fprintf(stderr, "Valid types: auto, pg_basebackup, pg_probackup (case-insensitive)\n");
		return EXIT_INVALID_ARGUMENTS;
	}

	/* Validate format */
	if (strcmp(opts->format, "table") != 0 &&
		strcmp(opts->format, "json") != 0 &&
		strcmp(opts->format, "yaml") != 0)
	{
		fprintf(stderr, "Error: Invalid format: %s\n", opts->format);
		fprintf(stderr, "Valid formats: table, json, yaml\n");
		return EXIT_INVALID_ARGUMENTS;
	}

	return EXIT_SUCCESS;
}

/*
 * Check if backup matches filter criteria
 * Returns true if backup should be included, false if filtered out
 */
static bool
matches_filters(const BackupInfo *backup, const ListOptions *opts)
{
	/* Filter by tool type */
	if (strcasecmp(opts->type_filter, "auto") != 0)
	{
		if (strcasecmp(opts->type_filter, "pg_basebackup") == 0 &&
			backup->tool != BACKUP_TOOL_PG_BASEBACKUP)
			return false;
		if (strcasecmp(opts->type_filter, "pg_probackup") == 0 &&
			backup->tool != BACKUP_TOOL_PG_PROBACKUP)
			return false;
	}

	/* Filter by status */
	if (strcasecmp(opts->status_filter, "all") != 0)
	{
		if (strcasecmp(opts->status_filter, "ok") == 0 &&
			backup->status != BACKUP_STATUS_OK)
			return false;
		if (strcasecmp(opts->status_filter, "error") == 0 &&
			backup->status != BACKUP_STATUS_ERROR)
			return false;
		if (strcasecmp(opts->status_filter, "warning") == 0 &&
			backup->status != BACKUP_STATUS_WARNING)
			return false;
		if (strcasecmp(opts->status_filter, "corrupt") == 0 &&
			backup->status != BACKUP_STATUS_CORRUPT)
			return false;
		if (strcasecmp(opts->status_filter, "orphan") == 0 &&
			backup->status != BACKUP_STATUS_ORPHAN)
			return false;
	}

	return true;
}

/*
 * Compare two backups by start_time for sorting
 * Returns: <0 if a < b, 0 if a == b, >0 if a > b
 */
static int
compare_backups_by_time(const void *a, const void *b)
{
	const BackupInfo *backup_a = *(const BackupInfo **)a;
	const BackupInfo *backup_b = *(const BackupInfo **)b;

	/* Handle missing start_time (treat as 0) */
	time_t time_a = backup_a->start_time;
	time_t time_b = backup_b->start_time;

	if (time_a < time_b)
		return -1;
	else if (time_a > time_b)
		return 1;
	else
		return 0;
}

/*
 * Compare two backups by end_time for sorting
 * Returns: <0 if a < b, 0 if a == b, >0 if a > b
 */
static int
compare_backups_by_end_time(const void *a, const void *b)
{
	const BackupInfo *backup_a = *(const BackupInfo **)a;
	const BackupInfo *backup_b = *(const BackupInfo **)b;

	time_t time_a = backup_a->end_time;
	time_t time_b = backup_b->end_time;

	/* Treat 0 as "in progress" - should sort to end */
	if (time_a == 0 && time_b != 0)
		return 1;
	if (time_a != 0 && time_b == 0)
		return -1;

	if (time_a < time_b)
		return -1;
	else if (time_a > time_b)
		return 1;
	else
		return 0;
}

/*
 * Compare two backups by backup_id (alphabetically)
 * Returns: <0 if a < b, 0 if a == b, >0 if a > b
 */
static int
compare_backups_by_name(const void *a, const void *b)
{
	const BackupInfo *backup_a = *(const BackupInfo **)a;
	const BackupInfo *backup_b = *(const BackupInfo **)b;

	return strcmp(backup_a->backup_id, backup_b->backup_id);
}

/*
 * Compare two backups by size (data_bytes)
 * Returns: <0 if a < b, 0 if a == b, >0 if a > b
 */
static int
compare_backups_by_size(const void *a, const void *b)
{
	const BackupInfo *backup_a = *(const BackupInfo **)a;
	const BackupInfo *backup_b = *(const BackupInfo **)b;

	uint64_t size_a = backup_a->data_bytes;
	uint64_t size_b = backup_b->data_bytes;

	if (size_a < size_b)
		return -1;
	else if (size_a > size_b)
		return 1;
	else
		return 0;
}

/*
 * Sort backup list by specified field
 * Creates an array of pointers, sorts them, then rebuilds the linked list
 */
static BackupInfo*
sort_backups(BackupInfo *backups, const char *sort_by, bool reverse)
{
	int (*comparator)(const void *, const void *);

	if (backups == NULL)
		return NULL;

	/* Select comparator based on sort_by */
	if (strcasecmp(sort_by, "time") == 0)
		comparator = compare_backups_by_time;
	else if (strcasecmp(sort_by, "end_time") == 0)
		comparator = compare_backups_by_end_time;
	else if (strcasecmp(sort_by, "name") == 0)
		comparator = compare_backups_by_name;
	else if (strcasecmp(sort_by, "size") == 0)
		comparator = compare_backups_by_size;
	else
		comparator = compare_backups_by_time;  /* Default to time */

	/* Count backups */
	int count = 0;
	BackupInfo *current = backups;
	while (current != NULL)
	{
		count++;
		current = current->next;
	}

	if (count <= 1)
		return backups;  /* Nothing to sort */

	/* Create array of pointers */
	BackupInfo **array = malloc(count * sizeof(BackupInfo *));
	if (array == NULL)
		return backups;  /* Out of memory, return unsorted */

	/* Fill array */
	current = backups;
	for (int i = 0; i < count; i++)
	{
		array[i] = current;
		current = current->next;
	}

	/* Sort array */
	qsort(array, count, sizeof(BackupInfo *), comparator);

	/* Rebuild linked list */
	if (reverse)
	{
		/* Reverse order: newest first */
		for (int i = count - 1; i > 0; i--)
			array[i]->next = array[i - 1];
		array[0]->next = NULL;
		backups = array[count - 1];
	}
	else
	{
		/* Normal order: oldest first */
		for (int i = 0; i < count - 1; i++)
			array[i]->next = array[i + 1];
		array[count - 1]->next = NULL;
		backups = array[0];
	}

	free(array);
	return backups;
}

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
print_table_header(void)
{
	printf("%-20s %-16s %-12s %-16s %-8s %-19s %-19s %-10s %-10s\n",
		   "BACKUP ID", "NODE", "TYPE", "TOOL", "STATUS", "START TIME", "END TIME", "SIZE", "WAL SIZE");
	printf("%-20s %-16s %-12s %-16s %-8s %-19s %-19s %-10s %-10s\n",
		   "--------------------", "----------------", "------------", "----------------", "--------",
		   "-------------------", "-------------------", "----------", "----------");
}

static void
print_backup_table_row(const BackupInfo *backup)
{
	char start_time[20];
	char end_time[20];
	char size_str[20];
	char wal_size_str[20];

	/* Format timestamps */
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

	/* Format size */
	if (backup->data_bytes > 0)
	{
		double size_mb = backup->data_bytes / (1024.0 * 1024.0);
		if (size_mb > 1024)
			snprintf(size_str, sizeof(size_str), "%.2f GB", size_mb / 1024.0);
		else
			snprintf(size_str, sizeof(size_str), "%.2f MB", size_mb);
	}
	else
		snprintf(size_str, sizeof(size_str), "N/A");

	/* Format WAL size */
	if (backup->wal_bytes > 0)
	{
		double wal_mb = backup->wal_bytes / (1024.0 * 1024.0);
		if (wal_mb > 1024)
			snprintf(wal_size_str, sizeof(wal_size_str), "%.2f GB", wal_mb / 1024.0);
		else
			snprintf(wal_size_str, sizeof(wal_size_str), "%.2f MB", wal_mb);
	}
	else
		snprintf(wal_size_str, sizeof(wal_size_str), "-");

	printf("%-20s %-16s %-12s %-16s %s%-8s%s %-19s %-19s %-10s %-10s\n",
		   backup->backup_id,
		   backup->node_name[0] ? backup->node_name : "localhost",
		   backup_type_to_string(backup->type),
		   backup_tool_to_string(backup->tool),
		   get_status_color(backup->status),
		   backup_status_to_string(backup->status),
		   use_color ? COLOR_RESET : "",
		   start_time,
		   end_time,
		   size_str,
		   wal_size_str);
}

/*
 * output_directory_group - Output backups from a single directory
 *
 * Parameters:
 * - directory_path: Path to the directory containing backups
 * - backups: Linked list of BackupInfo structures for this directory
 * - opts: List options including limit
 *
 * Returns:
 * - OutputStats structure containing count and total_bytes for this directory
 */
static OutputStats
output_directory_group(const char *directory_path, const BackupInfo *backups, const ListOptions *opts)
{
	const BackupInfo *current = backups;
	OutputStats stats = {0, 0};

	/* Print directory header */
	printf("\nDirectory: %s\n", directory_path);

	/* If this is a pg_probackup instance, also show instance name */
	if (current != NULL && current->instance_name[0] != '\0')
	{
		printf("Instance: %s\n", current->instance_name);
	}

	print_table_header();

	/* Print all backups in this directory */
	while (current != NULL)
	{
		print_backup_table_row(current);
		stats.count++;
		stats.total_bytes += current->data_bytes;
		if (opts->limit > 0 && stats.count >= opts->limit)
			break;
		current = current->next;
	}

	return stats;
}

/*
 * Helper function to extract parent directory from backup path
 */
static void
get_parent_directory(const char *backup_path, char *parent_dir, size_t parent_dir_size)
{
	char *last_slash;

	if (backup_path == NULL || backup_path[0] == '\0')
	{
		parent_dir[0] = '\0';
		return;
	}

	/* Copy the path */
	snprintf(parent_dir, parent_dir_size, "%s", backup_path);

	/* Find last slash and truncate */
	last_slash = strrchr(parent_dir, '/');
	if (last_slash != NULL)
		*last_slash = '\0';
	else
		parent_dir[0] = '\0';
}

/*
 * output_backups - Output backup list in specified format
 *
 * Groups backups by parent directory and outputs each group
 * as a separate table.
 *
 * Iterates through the backup list and outputs each backup according to the
 * requested format (table, JSON, or YAML). Currently only table format
 * is fully implemented.
 *
 * During iteration, accumulates statistics:
 * - Total number of backups displayed
 * - Total size of all backups in bytes
 *
 * Respects the --limit option to cap the number of backups shown.
 *
 * Parameters:
 * - backups: Linked list of BackupInfo structures to output
 * - opts: List options including format and limit
 *
 * Returns:
 * - OutputStats structure containing count and total_bytes
 */
static OutputStats
output_backups(const BackupInfo *backups, const ListOptions *opts)
{
	const BackupInfo *current;
	OutputStats stats = {0, 0};
	OutputStats dir_stats;

	if (strcmp(opts->format, "table") == 0)
	{
		/*
		 * Group backups by parent directory
		 * First pass: find all unique parent directories
		 */
		char directory_paths[100][PATH_MAX];  /* Max 100 directories */
		int num_directories = 0;

		/* Collect unique parent directories */
		current = backups;
		while (current != NULL)
		{
			/* Apply filters */
			if (matches_filters(current, opts))
			{
				char parent_dir[PATH_MAX];
				get_parent_directory(current->backup_path, parent_dir, sizeof(parent_dir));

				if (parent_dir[0] != '\0')
				{
					/* Check if we've seen this directory before */
					bool found = false;
					for (int i = 0; i < num_directories; i++)
					{
						if (strcmp(directory_paths[i], parent_dir) == 0)
						{
							found = true;
							break;
						}
					}
					if (!found && num_directories < 100)
					{
						snprintf(directory_paths[num_directories], PATH_MAX, "%s", parent_dir);
						num_directories++;
					}
				}
			}
			current = current->next;
		}

		/* Sort directory paths lexicographically */
		if (num_directories > 1)
		{
			for (int i = 0; i < num_directories - 1; i++)
			{
				for (int j = i + 1; j < num_directories; j++)
				{
					if (strcmp(directory_paths[i], directory_paths[j]) > 0)
					{
						/* Swap */
						char temp[PATH_MAX];
						snprintf(temp, PATH_MAX, "%s", directory_paths[i]);
						snprintf(directory_paths[i], PATH_MAX, "%s", directory_paths[j]);
						snprintf(directory_paths[j], PATH_MAX, "%s", temp);
					}
				}
			}
		}

		/* Output each directory group */
		for (int i = 0; i < num_directories; i++)
		{
			/* Build a list of backups for this directory */
			BackupInfo *dir_list = NULL;
			BackupInfo *dir_tail = NULL;

			current = backups;
			while (current != NULL)
			{
				if (matches_filters(current, opts))
				{
					char parent_dir[PATH_MAX];
					get_parent_directory(current->backup_path, parent_dir, sizeof(parent_dir));

					if (strcmp(parent_dir, directory_paths[i]) == 0)
					{
						/* Create a copy for temporary list */
						BackupInfo *copy = (BackupInfo *) malloc(sizeof(BackupInfo));
						if (copy != NULL)
						{
							memcpy(copy, current, sizeof(BackupInfo));
							copy->next = NULL;

							if (dir_list == NULL)
								dir_list = copy;
							else
								dir_tail->next = copy;
							dir_tail = copy;
						}
					}
				}
				current = current->next;
			}

			if (dir_list != NULL)
			{
				/* Sort backups */
				dir_list = sort_backups(dir_list, opts->sort_by, opts->reverse);

				dir_stats = output_directory_group(directory_paths[i], dir_list, opts);
				stats.count += dir_stats.count;
				stats.total_bytes += dir_stats.total_bytes;

				/* Free temporary list */
				free_backup_list(dir_list);
			}
		}
	}
	else if (strcmp(opts->format, "json") == 0)
	{
		/* TODO: Implement JSON output (Phase 2) */
		fprintf(stderr, "Warning: JSON output not yet implemented\n");
		fprintf(stderr, "Falling back to table format\n\n");
		return output_backups(backups, opts);  /* Recursive call with table format */
	}
	else
	{
		/* YAML - not implemented yet */
		fprintf(stderr, "Warning: %s output not yet implemented\n", opts->format);
		fprintf(stderr, "Falling back to table format\n\n");
		return output_backups(backups, opts);  /* Recursive call with table format */
	}

	return stats;
}

/*
 * cmd_list_main - Main function for the 'list' command
 *
 * Executes the complete list command workflow:
 * 1. Parse command-line arguments (--backup-dir, --type, --status, etc.)
 * 2. Scan backup directory via scan_backup_directory()
 * 3. TODO: Apply filters by tool type and status (currently not implemented)
 * 4. TODO: Sort results by selected field (currently not implemented)
 * 5. Output formatted backup list as a table
 * 6. Display summary statistics (backup count and total size)
 *
 * CURRENT LIMITATIONS:
 * - Filtering by --type and --status: arguments are parsed but not applied
 * - Sorting by --sort-by: arguments are parsed but not applied
 * - Only --limit is functional
 * - Only table format is implemented (JSON/YAML fallback to table)
 *
 * Returns:
 * - EXIT_SUCCESS (0) on successful execution
 * - EXIT_FAILURE (1) on error (directory not found, no backups)
 * - 4 on invalid arguments
 */
int
cmd_list_main(int argc, char **argv)
{
	ListOptions opts;
	BackupInfo *backups = NULL;
	int ret;
	OutputStats stats;
	char total_size_str[64];

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

	/* Log what we're doing */
	log_info("Scanning backup directory: %s", opts.backup_dir);

	/* Scan backup directory */
	backups = scan_backup_directory(opts.backup_dir, opts.max_depth);

	if (backups == NULL)
	{
		fprintf(stderr, "No backups found in %s\n", opts.backup_dir);
		log_info("No backups found");
		return EXIT_NO_BACKUPS_FOUND;
	}

	/* Sort backups by time */
	backups = sort_backups(backups, opts.sort_by, opts.reverse);

	/* Output results */
	stats = output_backups(backups, &opts);

	/* Format total size */
	if (stats.total_bytes > 0)
	{
		double size_mb = stats.total_bytes / (1024.0 * 1024.0);
		if (size_mb > 1024)
		{
			double size_gb = size_mb / 1024.0;
			if (size_gb > 1024)
				snprintf(total_size_str, sizeof(total_size_str), "%.2f TB", size_gb / 1024.0);
			else
				snprintf(total_size_str, sizeof(total_size_str), "%.2f GB", size_gb);
		}
		else
			snprintf(total_size_str, sizeof(total_size_str), "%.2f MB", size_mb);
	}
	else
		snprintf(total_size_str, sizeof(total_size_str), "N/A");

	/* Summary */
	printf("\nTotal backups found: %d\n", stats.count);
	printf("Total size: %s\n", total_size_str);
	log_info("Total backups found: %d, total size: %s", stats.count, total_size_str);

	/* Cleanup */
	free_backup_list(backups);

	return EXIT_SUCCESS;
}
