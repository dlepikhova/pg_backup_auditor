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
	bool no_recurse_seen = false;

	static struct option long_options[] = {
		{"backup-dir",  required_argument, 0, 'B'},
		{"type",        required_argument, 0, 't'},
		{"status",      required_argument, 0, 's'},
		{"format",      required_argument, 0, 'f'},
		{"sort-by",     required_argument, 0,  0 },
		{"reverse",     no_argument,       0, 'r'},
		{"limit",       required_argument, 0, 'n'},
		{"max-depth",   required_argument, 0, 'd'},
		{"no-recurse",  no_argument,       0, 'R'},
		{"help",        no_argument,       0, 'h'},
		{0, 0, 0, 0}
	};

	while ((c = getopt_long(argc, argv, "B:t:s:f:rn:d:Rh",
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
			case 'R':
				if (check_duplicate_option(no_recurse_seen, "--no-recurse"))
					return EXIT_INVALID_ARGUMENTS;
				opts->max_depth = 0;
				no_recurse_seen = true;
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
		strcasecmp(opts->type_filter, "pg_probackup") != 0 &&
		strcasecmp(opts->type_filter, "pgbackrest") != 0)
	{
		fprintf(stderr, "Error: Invalid type filter: %s\n", opts->type_filter);
		fprintf(stderr, "Valid types: auto, pg_basebackup, pg_probackup, pgbackrest (case-insensitive)\n");
		return EXIT_INVALID_ARGUMENTS;
	}

	/* Validate format */
	if (strcmp(opts->format, "table") != 0)
	{
		fprintf(stderr, "Error: Invalid format: %s\n", opts->format);
		fprintf(stderr, "Valid formats: table\n");
		return EXIT_INVALID_ARGUMENTS;
	}

	/* Validate status filter */
	if (strcasecmp(opts->status_filter, "all")     != 0 &&
		strcasecmp(opts->status_filter, "ok")      != 0 &&
		strcasecmp(opts->status_filter, "warning") != 0 &&
		strcasecmp(opts->status_filter, "error")   != 0 &&
		strcasecmp(opts->status_filter, "corrupt") != 0 &&
		strcasecmp(opts->status_filter, "orphan")  != 0 &&
		strcasecmp(opts->status_filter, "running") != 0)
	{
		fprintf(stderr, "Error: Invalid status filter: %s\n", opts->status_filter);
		fprintf(stderr, "Valid values: all, ok, warning, error, corrupt, orphan, running\n");
		return EXIT_INVALID_ARGUMENTS;
	}

	/* Validate sort_by */
	if (strcasecmp(opts->sort_by, "time")       != 0 &&
		strcasecmp(opts->sort_by, "start_time") != 0 &&
		strcasecmp(opts->sort_by, "end_time")   != 0 &&
		strcasecmp(opts->sort_by, "name")        != 0 &&
		strcasecmp(opts->sort_by, "size")        != 0)
	{
		fprintf(stderr, "Error: Invalid sort field: %s\n", opts->sort_by);
		fprintf(stderr, "Valid values: start_time, end_time, name, size\n");
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
		if (strcasecmp(opts->type_filter, "pgbackrest") == 0 &&
			backup->tool != BACKUP_TOOL_PGBACKREST)
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
		if (strcasecmp(opts->status_filter, "running") == 0 &&
			backup->status != BACKUP_STATUS_RUNNING)
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
	if (strcasecmp(sort_by, "time") == 0 || strcasecmp(sort_by, "start_time") == 0)
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
	printf("%-20s %-16s %-12s %-16s %-4s %-8s %-19s %-19s %-10s %-10s\n",
		   "BACKUP ID", "NODE", "TYPE", "TOOL", "PG", "STATUS", "START TIME", "END TIME", "SIZE", "WAL SIZE");
	printf("%-20s %-16s %-12s %-16s %-4s %-8s %-19s %-19s %-10s %-10s\n",
		   "--------------------", "----------------", "------------", "----------------", "----", "--------",
		   "-------------------", "-------------------", "----------", "----------");
}

/*
 * UTF-8 box-drawing characters used for the tree display.
 *
 * Each character is 3 bytes but occupies 1 terminal column, so any string
 * containing them needs extra width padding in printf("%-*s", width, str).
 * Extra bytes = (byte_length - display_columns).
 *
 *   │  = E2 94 82  — vertical line (continuation)
 *   ├  = E2 94 9C  — tee (non-last child)
 *   └  = E2 94 94  — corner (last child)
 *   ─  = E2 94 80  — horizontal line
 *
 * Patterns used per level (3 display columns each):
 *   "│  "   5 bytes, 3 cols → +2 extra
 *   "   "   3 bytes, 3 cols → +0 extra
 *   "├─ "   7 bytes, 3 cols → +4 extra  (connector for non-last child)
 *   "└─ "   7 bytes, 3 cols → +4 extra  (connector for last child)
 */
#define TREE_VERT  "\xe2\x94\x82"   /* │ */
#define TREE_TEE   "\xe2\x94\x9c"   /* ├ */
#define TREE_LAST  "\xe2\x94\x94"   /* └ */
#define TREE_HORIZ "\xe2\x94\x80"   /* ─ */

/*
 * Print one backup row.
 *
 * prefix      — tree prefix string to display before the backup_id
 *               (e.g. "│  ├─ ", empty for root)
 * extra_bytes — byte length of prefix minus its display width; used to
 *               compensate printf's %-Ns field width for multi-byte chars
 */
static void
print_backup_table_row_tree(const BackupInfo *backup,
							const char *prefix, int extra_bytes)
{
	char start_time[20];
	char end_time[20];
	char size_str[20];
	char wal_size_str[20];
	char pg_ver_str[8];
	char id_field[128];

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

	if (backup->pg_version > 0)
		snprintf(pg_ver_str, sizeof(pg_ver_str), "%d", backup->pg_version / 10000);
	else
		snprintf(pg_ver_str, sizeof(pg_ver_str), "-");

	snprintf(id_field, sizeof(id_field), "%s%s", prefix, backup->backup_id);

	printf("%-*s %-16s %-12s %-16s %-4s %s%-8s%s %-19s %-19s %-10s %-10s\n",
		   20 + extra_bytes,
		   id_field,
		   backup->node_name[0] ? backup->node_name : "localhost",
		   backup_type_to_string(backup->type),
		   backup_tool_to_string(backup->tool),
		   pg_ver_str,
		   get_status_color(backup->status),
		   backup_status_to_string(backup->status),
		   use_color ? COLOR_RESET : "",
		   start_time,
		   end_time,
		   size_str,
		   wal_size_str);
}

/*
 * Collect direct children of parent_id from arr[0..count-1], sorted by
 * start_time.  Returns number of children found.
 */
static int
collect_children(BackupInfo **arr, int count, const char *parent_id,
				 BackupInfo **out, int capacity)
{
	int n = 0;
	for (int i = 0; i < count && n < capacity; i++)
	{
		if (arr[i]->parent_backup_id[0] != '\0' &&
			strcmp(arr[i]->parent_backup_id, parent_id) == 0)
			out[n++] = arr[i];
	}
	if (n > 1)
		qsort(out, n, sizeof(BackupInfo *), compare_backups_by_time);
	return n;
}

/*
 * Recursively print a backup and all its descendants using proper tree
 * drawing (├─ for non-last children, └─ for the last child, │ for
 * vertical continuation lines at intermediate levels).
 *
 * indent       — prefix string inherited from the parent level;
 *                starts empty for root nodes and grows with each level.
 * indent_extra — extra bytes in `indent` due to multi-byte UTF-8 chars.
 * is_root      — true for FULL backups (no connector drawn).
 * is_last      — true if this node is the last child of its parent.
 */
static void
print_chain_recursive(BackupInfo **arr, int count, bool *visited,
					  BackupInfo *backup,
					  const char *indent, int indent_extra,
					  bool is_root, bool is_last,
					  const ListOptions *opts, OutputStats *stats)
{
	/* Mark visited */
	for (int i = 0; i < count; i++)
		if (arr[i] == backup) { visited[i] = true; break; }

	if (opts->limit > 0 && stats->count >= opts->limit)
		return;

	stats->count++;
	stats->total_bytes += backup->data_bytes;

	/* Build display prefix: indent + connector */
	char display_prefix[256];
	int  display_extra;

	if (is_root)
	{
		display_prefix[0] = '\0';
		display_extra = 0;
	}
	else
	{
		/* connector: "└─ " (last) or "├─ " (non-last), both +4 extra bytes */
		const char *connector = is_last
			? TREE_LAST TREE_HORIZ " "
			: TREE_TEE  TREE_HORIZ " ";
		snprintf(display_prefix, sizeof(display_prefix), "%s%s", indent, connector);
		display_extra = indent_extra + 4;
	}

	print_backup_table_row_tree(backup, display_prefix, display_extra);

	/* Collect children */
	BackupInfo *children[256];
	int nchildren = collect_children(arr, count, backup->backup_id,
									 children, 256);
	if (nchildren == 0)
		return;

	/*
	 * Build indent for the next level.
	 * If this node is the last child, no vertical line continues → "   ".
	 * Otherwise a vertical line continues → "│  " (+2 extra bytes).
	 */
	char child_indent[256];
	int  child_indent_extra;

	if (is_root)
	{
		child_indent[0] = '\0';
		child_indent_extra = 0;
	}
	else
	{
		const char *cont = is_last ? "   " : TREE_VERT "  ";
		snprintf(child_indent, sizeof(child_indent), "%s%s", indent, cont);
		child_indent_extra = indent_extra + (is_last ? 0 : 2);
	}

	for (int i = 0; i < nchildren; i++)
	{
		if (opts->limit > 0 && stats->count >= opts->limit)
			break;

		/* Cycle guard */
		bool already = false;
		for (int j = 0; j < count; j++)
			if (arr[j] == children[i] && visited[j]) { already = true; break; }
		if (already)
			continue;

		print_chain_recursive(arr, count, visited, children[i],
							  child_indent, child_indent_extra,
							  false, (i == nchildren - 1),
							  opts, stats);
	}
}

/*
 * output_directory_group - Output backups from a single directory
 *
 * Displays backups grouped by chain: each FULL backup is followed by its
 * incremental descendants indented with tree characters (└─).  Backups
 * within a chain are ordered by start_time; orphaned incrementals (no
 * FULL ancestor in this group) are printed last at depth 0.
 */
static OutputStats
output_directory_group(const char *directory_path, BackupInfo *backups,
					   const ListOptions *opts)
{
	OutputStats stats = {0, 0};

	/* Print directory header */
	printf("\nDirectory: %s\n", directory_path);
	if (backups != NULL && backups->instance_name[0] != '\0')
		printf("Instance: %s\n", backups->instance_name);
	print_table_header();

	/* Convert linked list to array for random-access traversal */
	int count = 0;
	for (BackupInfo *b = backups; b != NULL; b = b->next)
		count++;
	if (count == 0)
		return stats;

	BackupInfo **arr    = malloc(count * sizeof(BackupInfo *));
	bool        *visited = calloc(count, sizeof(bool));
	if (arr == NULL || visited == NULL)
	{
		free(arr);
		free(visited);
		return stats;
	}

	int idx = 0;
	for (BackupInfo *b = backups; b != NULL; b = b->next)
		arr[idx++] = b;

	/* Sort by start_time for stable chain traversal */
	qsort(arr, count, sizeof(BackupInfo *), compare_backups_by_time);

	/* Print FULL backups (chain roots) with their incremental subtrees */
	for (int j = 0; j < count; j++)
	{
		if (visited[j] || arr[j]->type != BACKUP_TYPE_FULL)
			continue;
		if (opts->limit > 0 && stats.count >= opts->limit)
			break;
		print_chain_recursive(arr, count, visited, arr[j],
							  "", 0, true, true, opts, &stats);
	}

	/* Print any unvisited backups (incrementals whose FULL is missing/elsewhere),
	 * sorted by start_lsn so LSN order is preserved even without a chain root. */
	{
		BackupInfo *orphans[1024];
		int nordans = 0;
		for (int j = 0; j < count && nordans < (int)(sizeof(orphans)/sizeof(*orphans)); j++)
			if (!visited[j])
				orphans[nordans++] = arr[j];

		/* Sort orphans by start_lsn ascending */
		for (int a = 0; a < nordans - 1; a++)
			for (int b = a + 1; b < nordans; b++)
				if (orphans[a]->start_lsn > orphans[b]->start_lsn)
				{
					BackupInfo *tmp = orphans[a];
					orphans[a] = orphans[b];
					orphans[b] = tmp;
				}

		for (int j = 0; j < nordans; j++)
		{
			if (opts->limit > 0 && stats.count >= opts->limit)
				break;
			print_chain_recursive(arr, count, visited, orphans[j],
								  "", 0, true, true, opts, &stats);
		}
	}

	free(arr);
	free(visited);
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
						memcpy(temp, directory_paths[i], PATH_MAX);
						memcpy(directory_paths[i], directory_paths[j], PATH_MAX);
						memcpy(directory_paths[j], temp, PATH_MAX);
					}
				}
			}
		}

		/* Output each directory group */
		for (int i = 0; i < num_directories; i++)
		{
			/* Stop early if global limit already reached */
			if (opts->limit > 0 && stats.count >= opts->limit)
				break;

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
				/* Remaining slots under the global limit */
				int remaining = (opts->limit > 0) ? (opts->limit - stats.count) : 0;

				/* Temporarily override limit for this group */
				ListOptions group_opts = *opts;
				group_opts.limit = remaining;

				/* sort_by is ignored inside output_directory_group — chains
				 * are always displayed in start_time order for coherent tree
				 * traversal.  The external sort still affects FULL ordering
				 * when all backups share the same start_time (rare). */
				dir_list  = sort_backups(dir_list, opts->sort_by, opts->reverse);
				dir_stats = output_directory_group(directory_paths[i], dir_list, &group_opts);
				stats.count += dir_stats.count;
				stats.total_bytes += dir_stats.total_bytes;

				free_backup_list(dir_list);
			}
		}
	}
	/* format is validated before reaching here — only "table" is accepted */

	return stats;
}

/*
 * cmd_list_main - Main function for the 'list' command
 *
 * Executes the complete list command workflow:
 * 1. Parse command-line arguments (--backup-dir, --type, --status, etc.)
 * 2. Scan backup directory via scan_backup_directory()
 * 3. Sort results by selected field (sort_backups)
 * 4. Output filtered and limited backup list as a table (output_backups)
 * 5. Display summary statistics (backup count and total size)
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

	/* Output results (sorting is done per-directory group inside output_backups) */
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
