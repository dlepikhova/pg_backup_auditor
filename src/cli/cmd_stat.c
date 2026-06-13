/*
 * cmd_stat.c
 *
 * Implementation of 'stat' command - backup collection statistics
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
#include "cmd_help.h"
#include "arg_parser.h"
#include "adapter.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <getopt.h>
#include <math.h>
#include <limits.h>
#include <dirent.h>
#include <sys/stat.h>

typedef struct {
	char *backup_dir;
	char *wal_archive;
} StatOptions;

typedef struct {
	BackupTool tool;
	BackupType type;
	char       instance_name[64];
	int        count;
	uint64_t   total_bytes;
	uint64_t   sum_sizes_sq;
	uint64_t   total_wal_bytes;
	time_t     min_time;
	time_t     max_time;
	int64_t    total_duration;
	int        duration_count;
	int        ok_count;
} StatGroup;

typedef struct {
	time_t   start_time;
	uint64_t data_bytes;
} FullEntry;

typedef struct {
	BackupTool tool;
	char       instance_name[64];
	FullEntry  fulls[200];
	int        full_count;
} GrowthTrack;

static void
init_options(StatOptions *opts)
{
	opts->backup_dir = NULL;
	opts->wal_archive = NULL;
}

static int
parse_arguments(int argc, char **argv, StatOptions *opts)
{
	int c;
	int option_index = 0;
	bool backup_dir_seen = false;

	static struct option long_options[] = {
		{"backup-dir",   required_argument, 0, 'B'},
		{"wal-archive",  required_argument, 0, 'W'},
		{"help",         no_argument,       0, 'h'},
		{0, 0, 0, 0}
	};

	while ((c = getopt_long(argc, argv, "B:W:h",
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
			case 'W':
				opts->wal_archive = optarg;
				break;
			case 'h':
				print_stat_usage();
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
validate_options(const StatOptions *opts)
{
	if (!validate_required_option(opts->backup_dir, "--backup-dir"))
		return EXIT_INVALID_ARGUMENTS;

	if (!is_directory(opts->backup_dir))
	{
		fprintf(stderr, "Error: Backup directory does not exist: %s\n", opts->backup_dir);
		return EXIT_GENERAL_ERROR;
	}

	return EXIT_SUCCESS;
}

static void
format_bytes(uint64_t bytes, char *buf, size_t size)
{
	if (bytes >= (uint64_t)1024 * 1024 * 1024)
		snprintf(buf, size, "%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
	else if (bytes >= (uint64_t)1024 * 1024)
		snprintf(buf, size, "%.1f MB", bytes / (1024.0 * 1024.0));
	else if (bytes >= 1024)
		snprintf(buf, size, "%.1f KB", bytes / 1024.0);
	else
		snprintf(buf, size, "%llu B", (unsigned long long)bytes);
}

static void
format_timestamp(time_t t, char *buf, size_t size)
{
	if (t == 0)
	{
		snprintf(buf, size, "N/A");
		return;
	}
	struct tm *tm_info = localtime(&t);
	strftime(buf, size, "%Y-%m-%d %H:%M:%S", tm_info);
}

static void
format_duration(double seconds, char *buf, size_t size)
{
	if (seconds < 0)
	{
		snprintf(buf, size, "N/A");
		return;
	}
	int secs = (int)seconds;
	if (secs >= 3600)
		snprintf(buf, size, "%dh %dm", secs / 3600, (secs % 3600) / 60);
	else if (secs >= 60)
		snprintf(buf, size, "%dm %ds", secs / 60, secs % 60);
	else
		snprintf(buf, size, "%ds", secs);
}

static uint64_t __attribute__((unused))
calculate_stddev(StatGroup *g)
{
	if (g->count < 2)
		return 0;

	double avg = (double)g->total_bytes / g->count;
	double variance = ((double)g->sum_sizes_sq / g->count) - (avg * avg);
	if (variance < 0)
		variance = 0;
	return (uint64_t)(sqrt(variance) + 0.5);
}

static StatGroup *
find_or_create_group(StatGroup *groups, int *group_count, int group_cap,
					 BackupTool tool, BackupType type, const char *instance_name)
{
	for (int i = 0; i < *group_count; i++)
	{
		if (groups[i].tool == tool && groups[i].type == type &&
			strcmp(groups[i].instance_name, instance_name) == 0)
			return &groups[i];
	}

	if (*group_count >= group_cap)
		return NULL;  /* Too many groups */

	StatGroup *g = &groups[*group_count];
	g->tool = tool;
	g->type = type;
	str_copy(g->instance_name, instance_name, sizeof(g->instance_name));
	g->count = 0;
	g->total_bytes = 0;
	g->sum_sizes_sq = 0;
	g->total_wal_bytes = 0;
	g->min_time = LLONG_MAX;
	g->max_time = 0;
	g->total_duration = 0;
	g->duration_count = 0;
	g->ok_count = 0;

	(*group_count)++;
	return g;
}

static void
free_stat_groups(StatGroup *groups, int group_count)
{
	(void)group_count;  /* unused */
	free(groups);
}

static int
compare_groups(const void *a, const void *b)
{
	const StatGroup *ga = (const StatGroup *)a;
	const StatGroup *gb = (const StatGroup *)b;

	if (ga->tool != gb->tool)
		return (int)ga->tool - (int)gb->tool;
	if (strcmp(ga->instance_name, gb->instance_name) != 0)
		return strcmp(ga->instance_name, gb->instance_name);
	return (int)ga->type - (int)gb->type;
}

static int
compare_full_entries(const void *a, const void *b)
{
	const FullEntry *fa = (const FullEntry *)a;
	const FullEntry *fb = (const FullEntry *)b;
	return (int)(fa->start_time - fb->start_time);
}

typedef struct {
	uint64_t total_wal;
	time_t min_time;
	time_t max_time;
} WalArchiveStats;

static WalArchiveStats __attribute__((unused))
analyze_wal_archive(const char *wal_archive_path)
{
	WalArchiveStats stats = {0, LLONG_MAX, 0};

	if (!wal_archive_path || !is_directory(wal_archive_path))
		return stats;

	DIR *dir = opendir(wal_archive_path);
	if (!dir)
		return stats;

	struct dirent *entry;
	while ((entry = readdir(dir)) != NULL)
	{
		if (entry->d_name[0] == '.')
			continue;
		if (strstr(entry->d_name, ".backup") != NULL)
			continue;

		char full_path[PATH_MAX];
		struct stat st;
		path_join(full_path, sizeof(full_path), wal_archive_path, entry->d_name);

		if (stat(full_path, &st) == 0 && S_ISREG(st.st_mode))
		{
			stats.total_wal += st.st_size;
			if (st.st_mtime < stats.min_time)
				stats.min_time = st.st_mtime;
			if (st.st_mtime > stats.max_time)
				stats.max_time = st.st_mtime;
		}
	}
	closedir(dir);
	return stats;
}

static void
print_growth_efficiency(BackupInfo *backups, StatGroup *groups, int group_count)
{
	GrowthTrack tracks[20];
	int track_count = 0;

	for (BackupInfo *b = backups; b != NULL; b = b->next)
	{
		if (b->type == BACKUP_TYPE_FULL && b->data_bytes > 0)
		{
			GrowthTrack *track = NULL;
			for (int i = 0; i < track_count; i++)
			{
				if (tracks[i].tool == b->tool &&
					strcmp(tracks[i].instance_name, b->instance_name) == 0)
				{
					track = &tracks[i];
					break;
				}
			}

			if (track == NULL)
			{
				if (track_count >= 20)
					continue;
				track = &tracks[track_count];
				track->tool = b->tool;
				str_copy(track->instance_name, b->instance_name, sizeof(track->instance_name));
				track->full_count = 0;
				track_count++;
			}

			if (track->full_count < 200)
			{
				track->fulls[track->full_count].start_time = b->start_time;
				track->fulls[track->full_count].data_bytes = b->data_bytes;
				track->full_count++;
			}
		}
	}

	if (track_count == 0)
		return;

	printf("\n");
	const char *col = use_color ? COLOR_CYAN : "";
	const char *rst = use_color ? COLOR_RESET : "";

	/* DATABASE GROWTH TREND */
	printf("%sDATABASE GROWTH TREND%s\n", col, rst);
	printf("  ──────────────────────────────────────────────────────────\n\n");

	for (int t = 0; t < track_count; t++)
	{
		GrowthTrack *track = &tracks[t];
		qsort(track->fulls, track->full_count, sizeof(FullEntry), compare_full_entries);

		printf("  %s", backup_tool_to_string(track->tool));
		if (track->instance_name[0] != '\0' && strcmp(track->instance_name, "localhost") != 0)
			printf(" / %s", track->instance_name);
		printf("\n");

		if (track->full_count == 0)
		{
			printf("    (no FULL backups)\n\n");
			continue;
		}
		else if (track->full_count == 1)
		{
			char size_str[32];
			format_bytes(track->fulls[0].data_bytes, size_str, sizeof(size_str));
			printf("    Only 1 valid FULL backup: %s\n\n", size_str);
			continue;
		}

		/* Calculate min/max/avg of FULL backup sizes (excluding zero-size) */
		uint64_t min_size = UINT64_MAX;
		uint64_t max_size = 0;
		uint64_t sum_size = 0;
		int valid_count = 0;

		for (int i = 0; i < track->full_count; i++)
		{
			uint64_t size = track->fulls[i].data_bytes;
			if (size > 0)
			{
				sum_size += size;
				if (size < min_size)
					min_size = size;
				if (size > max_size)
					max_size = size;
				valid_count++;
			}
		}

		if (valid_count == 0)
		{
			printf("    (no FULL backups with data)\n\n");
			continue;
		}

		uint64_t avg_size = sum_size / valid_count;

		char avg_str[32], min_str[32], max_str[32];
		format_bytes(avg_size, avg_str, sizeof(avg_str));
		format_bytes(min_size, min_str, sizeof(min_str));
		format_bytes(max_size, max_str, sizeof(max_str));

		printf("    FULL:  avg %s, min %s, max %s\n\n",
			   avg_str, min_str, max_str);
	}

	/* INCREMENTAL EFFICIENCY */
	printf("%sINCREMENTAL EFFICIENCY%s\n", col, rst);
	printf("  ──────────────────────────────────────────────────────────\n\n");

	for (int t = 0; t < track_count; t++)
	{
		GrowthTrack *track = &tracks[t];

		StatGroup *full_group = NULL;
		for (int i = 0; i < group_count; i++)
		{
			if (groups[i].tool == track->tool &&
				groups[i].type == BACKUP_TYPE_FULL &&
				strcmp(groups[i].instance_name, track->instance_name) == 0)
			{
				full_group = &groups[i];
				break;
			}
		}

		if (full_group == NULL || full_group->count == 0)
			continue;

		printf("  %s", backup_tool_to_string(track->tool));
		if (track->instance_name[0] != '\0' && strcmp(track->instance_name, "localhost") != 0)
			printf(" / %s", track->instance_name);
		printf("\n");

		uint64_t avg_full = full_group->total_bytes / full_group->count;
		bool has_incr = false;

		for (int i = 0; i < group_count; i++)
		{
			StatGroup *ig = &groups[i];
			if (ig->tool == track->tool &&
				strcmp(ig->instance_name, track->instance_name) == 0 &&
				ig->type != BACKUP_TYPE_FULL && ig->count > 0)
			{
				uint64_t avg_incr = ig->total_bytes / ig->count;
				if (avg_incr == 0)
					continue;

				int pct = (avg_full > 0) ? (int)(avg_incr * 100 / avg_full) : 0;

				char avg_incr_str[32], avg_full_str[32];
				format_bytes(avg_incr, avg_incr_str, sizeof(avg_incr_str));
				format_bytes(avg_full, avg_full_str, sizeof(avg_full_str));

				printf("    %s:  %3d%% of FULL  (avg %s vs %s FULL)\n",
					   backup_type_to_string(ig->type), pct, avg_incr_str, avg_full_str);
				has_incr = true;
			}
		}

		if (!has_incr)
			printf("    (no incremental backups)\n");

		printf("\n");
	}
}

int
cmd_stat_main(int argc, char **argv)
{
	StatOptions opts;
	BackupInfo *backups = NULL;
	StatGroup *groups = NULL;
	int group_count = 0;
	int ret;

	init_options(&opts);

	ret = parse_arguments(argc, argv, &opts);
	if (ret == EXIT_SUCCESS)   /* --help was shown */
		return EXIT_SUCCESS;
	if (ret != -1)             /* error occurred */
		return ret;

	ret = validate_options(&opts);
	if (ret != EXIT_SUCCESS)
		return ret;

	/* Scan backup directory */
	log_info("Scanning backup directory: %s", opts.backup_dir);
	backups = scan_backup_directory(opts.backup_dir, -1);
	if (backups == NULL)
	{
		fprintf(stderr, "Error: No backups found in: %s\n", opts.backup_dir);
		return EXIT_GENERAL_ERROR;
	}

	/* Allocate groups array (max 60 groups) */
	groups = malloc(60 * sizeof(StatGroup));
	if (groups == NULL)
	{
		fprintf(stderr, "Error: Memory allocation failed\n");
		free_backup_list(backups);
		return EXIT_GENERAL_ERROR;
	}

	/* Build groups */
	for (BackupInfo *b = backups; b != NULL; b = b->next)
	{
		StatGroup *g = find_or_create_group(groups, &group_count, 60,
											b->tool, b->type, b->instance_name);
		if (g == NULL)
		{
			fprintf(stderr, "Error: Too many backup type combinations (max 60)\n");
			free_stat_groups(groups, group_count);
			free_backup_list(backups);
			return EXIT_GENERAL_ERROR;
		}

		g->count++;
		uint64_t size = b->data_bytes + b->wal_bytes;
		g->total_bytes += size;
		g->sum_sizes_sq += size * size;
		g->total_wal_bytes += b->wal_bytes;

		if (b->start_time > 0)
		{
			if (b->start_time < g->min_time)
				g->min_time = b->start_time;
			if (b->start_time > g->max_time)
				g->max_time = b->start_time;
		}

		if (b->end_time > b->start_time)
		{
			int64_t duration = (int64_t)(b->end_time - b->start_time);
			if (duration < 48 * 3600)
			{
				g->total_duration += duration;
				g->duration_count++;
			}
		}
		if (b->status == BACKUP_STATUS_OK)
			g->ok_count++;
	}

	/* Sort groups for organized output */
	qsort(groups, group_count, sizeof(StatGroup), compare_groups);

	/* Header */
	{
		time_t now = time(NULL);
		char now_str[32];
		format_timestamp(now, now_str, sizeof(now_str));

		const char *title_col = use_color ? COLOR_CYAN : "";
		const char *rst = use_color ? COLOR_RESET : "";

		printf("%sBackup Statistics%s\n", title_col, rst);
		char resolved_dir[PATH_MAX];
		const char *display_dir = opts.backup_dir;
		if (realpath(opts.backup_dir, resolved_dir) != NULL)
			display_dir = resolved_dir;
		printf("Directory:  %s\n", display_dir);
		printf("Time:       %s\n\n", now_str);
	}

	/* Group and print statistics by tool and instance */
	BackupTool current_tool = (BackupTool)-1;
	char current_instance[64] = "";
	for (int i = 0; i < group_count; i++)
	{
		StatGroup *g = &groups[i];

		/* Print tool/instance header when switching tools or instances */
		if (g->tool != current_tool || strcmp(g->instance_name, current_instance) != 0)
		{
			if (current_tool != (BackupTool)-1)
				printf("\n");

			const char *col = use_color ? COLOR_CYAN : "";
			const char *rst = use_color ? COLOR_RESET : "";
			printf("%s%s", col, backup_tool_to_string(g->tool));
			if (g->instance_name[0] != '\0' && strcmp(g->instance_name, "localhost") != 0)
				printf(" / %s", g->instance_name);
			printf("%s\n", rst);

			printf("  Type            Count    Total Size   Avg Size   Avg Duration   OK%%\n");
			printf("  ───────────────────────────────────────────────────────────────────\n");
			current_tool = g->tool;
			str_copy(current_instance, g->instance_name, sizeof(current_instance));
		}

		/* Calculate statistics */
		uint64_t avg_bytes = g->total_bytes / g->count;
		double avg_dur = (g->duration_count > 0)
			? (double)g->total_duration / g->duration_count : -1.0;

		char total_str[32], avg_str[32], dur_str[16], ok_pct_str[8];
		format_bytes(g->total_bytes, total_str, sizeof(total_str));
		format_bytes(avg_bytes, avg_str, sizeof(avg_str));
		format_duration(avg_dur, dur_str, sizeof(dur_str));
		snprintf(ok_pct_str, sizeof(ok_pct_str), "%d%%",
				 g->count > 0 ? (g->ok_count * 100 / g->count) : 0);

		printf("  %-12s  %5d   %10s   %9s   %13s  %3s\n",
			   backup_type_to_string(g->type),
			   g->count,
			   total_str,
			   avg_str,
			   dur_str,
			   ok_pct_str);
	}

	/* Summary */
	{
		printf("\n");
		const char *col = use_color ? COLOR_CYAN : "";
		const char *rst = use_color ? COLOR_RESET : "";
		printf("%sSTORAGE%s\n", col, rst);

		/* Count by status */
		int ok_count = 0, warning_count = 0, error_count = 0, corrupt_count = 0, orphan_count = 0, running_count = 0;
		uint64_t ok_bytes = 0, warning_bytes = 0, error_bytes = 0, corrupt_bytes = 0, orphan_bytes = 0, running_bytes = 0;

		for (BackupInfo *b = backups; b != NULL; b = b->next)
		{
			uint64_t size = b->data_bytes + b->wal_bytes;
			switch (b->status)
			{
				case BACKUP_STATUS_OK:
					ok_count++;
					ok_bytes += size;
					break;
				case BACKUP_STATUS_WARNING:
					warning_count++;
					warning_bytes += size;
					break;
				case BACKUP_STATUS_ERROR:
					error_count++;
					error_bytes += size;
					break;
				case BACKUP_STATUS_CORRUPT:
					corrupt_count++;
					corrupt_bytes += size;
					break;
				case BACKUP_STATUS_ORPHAN:
					orphan_count++;
					orphan_bytes += size;
					break;
				case BACKUP_STATUS_RUNNING:
					running_count++;
					running_bytes += size;
					break;
			}
		}

		int total_count = ok_count + warning_count + error_count + corrupt_count + orphan_count + running_count;
		uint64_t total_bytes = ok_bytes + warning_bytes + error_bytes + corrupt_bytes + orphan_bytes + running_bytes;

		printf("  Status          Count   Size\n");
		printf("  ─────────────────────────────────\n");

		if (ok_count > 0)
		{
			char ok_str[32];
			format_bytes(ok_bytes, ok_str, sizeof(ok_str));
			const char *green = use_color ? COLOR_GREEN : "";
			const char *rst2 = use_color ? COLOR_RESET : "";
			printf("  %sOK%s              %5d   %s\n", green, rst2, ok_count, ok_str);
		}

		if (warning_count > 0)
		{
			char warn_str[32];
			format_bytes(warning_bytes, warn_str, sizeof(warn_str));
			const char *yellow = use_color ? COLOR_YELLOW : "";
			const char *rst2 = use_color ? COLOR_RESET : "";
			printf("  %sWARNING%s         %5d   %s\n", yellow, rst2, warning_count, warn_str);
		}

		if (error_count > 0)
		{
			char err_str[32];
			format_bytes(error_bytes, err_str, sizeof(err_str));
			const char *red = use_color ? COLOR_RED : "";
			const char *rst2 = use_color ? COLOR_RESET : "";
			printf("  %sERROR%s           %5d   %s\n", red, rst2, error_count, err_str);
		}

		if (corrupt_count > 0)
		{
			char corr_str[32];
			format_bytes(corrupt_bytes, corr_str, sizeof(corr_str));
			const char *red = use_color ? COLOR_RED : "";
			const char *rst2 = use_color ? COLOR_RESET : "";
			printf("  %sCORRUPT%s         %5d   %s\n", red, rst2, corrupt_count, corr_str);
		}

		if (orphan_count > 0)
		{
			char orph_str[32];
			format_bytes(orphan_bytes, orph_str, sizeof(orph_str));
			const char *yellow = use_color ? COLOR_YELLOW : "";
			const char *rst2 = use_color ? COLOR_RESET : "";
			printf("  %sORPHAN%s          %5d   %s\n", yellow, rst2, orphan_count, orph_str);
		}

		if (running_count > 0)
		{
			char run_str[32];
			format_bytes(running_bytes, run_str, sizeof(run_str));
			const char *blue = use_color ? COLOR_BLUE : "";
			const char *rst2 = use_color ? COLOR_RESET : "";
			printf("  %sRUNNING%s         %5d   %s\n", blue, rst2, running_count, run_str);
		}

		printf("  ─────────────────────────────────\n");
		char total_str[32];
		format_bytes(total_bytes, total_str, sizeof(total_str));
		printf("  TOTAL            %5d   %s\n", total_count, total_str);

		/* WAL Archive Volume by tool and instance (from backups or archives) */
		printf("\n  WAL Archive Volume:\n");

		typedef struct {
			BackupTool tool;
			char instance_name[64];
			uint64_t total_wal;
			time_t min_time;
			time_t max_time;
			bool from_archive;
		} ToolInstanceWal;

		ToolInstanceWal wals[20];
		int wal_count = 0;

		/* For pg_probackup: try to find and analyze WAL archives */
		typedef struct {
			char instance_name[64];
			WalArchiveStats archive_stats;
		} ProbackupArchive;

		ProbackupArchive pb_archives[20];
		int pb_archive_count = 0;

		for (BackupInfo *b = backups; b != NULL; b = b->next)
		{
			if (b->tool != BACKUP_TOOL_PG_PROBACKUP)
				continue;

			/* Check if we already processed this instance */
			bool found = false;
			for (int i = 0; i < pb_archive_count; i++)
			{
				if (strcmp(pb_archives[i].instance_name, b->instance_name) == 0)
				{
					found = true;
					break;
				}
			}
			if (found)
				continue;

			/* Try to find WAL archive for this instance */
			char wal_archive_path[PATH_MAX];

			/* First, try user-provided path */
			if (opts.wal_archive && is_directory(opts.wal_archive))
			{
				snprintf(wal_archive_path, sizeof(wal_archive_path), "%s", opts.wal_archive);
			}
			else
			{
				/* Try to find automatically from backup path
				 * backup_path is like: /path/to/catalog/backups/{instance}/{backup_id}
				 * WAL archive is at: /path/to/catalog/wal/{instance}
				 */
				char catalog_base[PATH_MAX];
				snprintf(catalog_base, sizeof(catalog_base), "%s", b->backup_path);

				/* Remove backup ID (last component) */
				char *last_slash = strrchr(catalog_base, '/');
				if (last_slash)
					*last_slash = '\0';

				/* Remove instance name (next-to-last component) */
				last_slash = strrchr(catalog_base, '/');
				if (last_slash)
					*last_slash = '\0';

				/* Now catalog_base is /path/to/catalog/backups */
				/* Navigate to /path/to/catalog/wal/{instance} */
				char wal_base[PATH_MAX];
				last_slash = strrchr(catalog_base, '/');
				if (last_slash)
				{
					*last_slash = '\0';  /* catalog_base is now /path/to/catalog */
					path_join(wal_base, sizeof(wal_base), catalog_base, "wal");
					path_join(wal_archive_path, sizeof(wal_archive_path), wal_base, b->instance_name);
				}
				else
				{
					/* Fallback: use backup_dir structure */
					char temp_base[PATH_MAX];
					path_join(temp_base, sizeof(temp_base), opts.backup_dir, "backups");
					path_join(wal_base, sizeof(wal_base), temp_base, "wal");
					path_join(wal_archive_path, sizeof(wal_archive_path), wal_base, b->instance_name);
				}
			}

			/* Analyze archive if found */
			if (is_directory(wal_archive_path))
			{
				if (pb_archive_count < 20)
				{
					WalArchiveStats stats = analyze_wal_archive(wal_archive_path);
					pb_archives[pb_archive_count].archive_stats = stats;
					str_copy(pb_archives[pb_archive_count].instance_name, b->instance_name,
							 sizeof(pb_archives[pb_archive_count].instance_name));
					pb_archive_count++;
				}
			}
		}

		/* Collect WAL stats from backups (only from backups with WAL) */
		for (BackupInfo *b = backups; b != NULL; b = b->next)
		{
			if (b->wal_bytes == 0)
				continue;  /* Skip backups without WAL */

			ToolInstanceWal *entry = NULL;
			for (int i = 0; i < wal_count; i++)
			{
				if (wals[i].tool == b->tool && strcmp(wals[i].instance_name, b->instance_name) == 0)
				{
					entry = &wals[i];
					break;
				}
			}

			if (entry == NULL)
			{
				if (wal_count >= 20)
					continue;
				entry = &wals[wal_count];
				entry->tool = b->tool;
				str_copy(entry->instance_name, b->instance_name, sizeof(entry->instance_name));
				entry->total_wal = 0;
				entry->min_time = LLONG_MAX;
				entry->max_time = 0;
				entry->from_archive = false;
				wal_count++;
			}

			entry->total_wal += b->wal_bytes;
			if (b->start_time > 0)
			{
				if (b->start_time < entry->min_time)
					entry->min_time = b->start_time;
				if (b->start_time > entry->max_time)
					entry->max_time = b->start_time;
			}
		}

		/* Use discovered WAL archives if they provide better data than backup metadata
		 * (for pg_probackup: backups may have wal_bytes but single timestamp, archives have time range)
		 */
		for (int i = 0; i < pb_archive_count; i++)
		{
			if (pb_archives[i].archive_stats.total_wal == 0)
				continue;

			/* Look for existing entry for this instance */
			for (int j = 0; j < wal_count; j++)
			{
				if (wals[j].tool == BACKUP_TOOL_PG_PROBACKUP &&
					strcmp(wals[j].instance_name, pb_archives[i].instance_name) == 0)
				{
					/* Found existing entry - check if archive has better time range */
					if (pb_archives[i].archive_stats.max_time > pb_archives[i].archive_stats.min_time &&
						(wals[j].max_time <= wals[j].min_time ||
						 (pb_archives[i].archive_stats.max_time - pb_archives[i].archive_stats.min_time) >
						 (wals[j].max_time - wals[j].min_time)))
					{
						wals[j].total_wal = pb_archives[i].archive_stats.total_wal;
						wals[j].min_time = pb_archives[i].archive_stats.min_time;
						wals[j].max_time = pb_archives[i].archive_stats.max_time;
						wals[j].from_archive = true;
					}
					goto next_archive;
				}
			}

			/* Not found - add new entry if space available */
			if (wal_count < 20)
			{
				ToolInstanceWal *entry = &wals[wal_count];
				entry->tool = BACKUP_TOOL_PG_PROBACKUP;
				str_copy(entry->instance_name, pb_archives[i].instance_name, sizeof(entry->instance_name));
				entry->total_wal = pb_archives[i].archive_stats.total_wal;
				entry->min_time = pb_archives[i].archive_stats.min_time;
				entry->max_time = pb_archives[i].archive_stats.max_time;
				entry->from_archive = true;
				wal_count++;
			}

			next_archive:
			(void)0;  /* Dummy statement for label */
		}

		/* Print WAL stats grouped by tool */
		BackupTool prev_tool = (BackupTool)-1;
		for (int i = 0; i < wal_count; i++)
		{
			if (wals[i].tool != prev_tool)
			{
				/* Calculate tool totals */
				uint64_t tool_total_wal = 0;
				time_t tool_min_time = LLONG_MAX;
				time_t tool_max_time = 0;

				for (int j = 0; j < wal_count; j++)
				{
					if (wals[j].tool == wals[i].tool)
					{
						tool_total_wal += wals[j].total_wal;
						if (wals[j].min_time < tool_min_time)
							tool_min_time = wals[j].min_time;
						if (wals[j].max_time > tool_max_time)
							tool_max_time = wals[j].max_time;
					}
				}

				if (tool_total_wal > 0 && tool_max_time > tool_min_time)
				{
					time_t days = (tool_max_time - tool_min_time) / 86400;
					if (days == 0)
						days = 1;
					uint64_t tool_wal_per_day = tool_total_wal / days;
					char tool_wal_str[32], total_wal_str[32];
					format_bytes(tool_wal_per_day, tool_wal_str, sizeof(tool_wal_str));
					format_bytes(tool_total_wal, total_wal_str, sizeof(total_wal_str));

					printf("    %s: %s/day\n", backup_tool_to_string(wals[i].tool), tool_wal_str);

					/* Print instances for this tool */
					for (int j = 0; j < wal_count; j++)
					{
						if (wals[j].tool == wals[i].tool && wals[j].total_wal > 0 &&
							wals[j].instance_name[0] != '\0' &&
							strcmp(wals[j].instance_name, "localhost") != 0)
						{
							/* Check if we have WAL archive for pg_probackup */
							ProbackupArchive *archive = NULL;
							if (wals[j].tool == BACKUP_TOOL_PG_PROBACKUP)
							{
								for (int k = 0; k < pb_archive_count; k++)
								{
									if (strcmp(pb_archives[k].instance_name, wals[j].instance_name) == 0 &&
										pb_archives[k].archive_stats.total_wal > 0)
									{
										archive = &pb_archives[k];
										break;
									}
								}
							}

							uint64_t wal_per_day = 0;
							if (archive && archive->archive_stats.max_time > archive->archive_stats.min_time)
							{
								time_t days = (archive->archive_stats.max_time - archive->archive_stats.min_time) / 86400;
								if (days == 0)
									days = 1;
								wal_per_day = archive->archive_stats.total_wal / days;
							}
							else if (wals[j].max_time > wals[j].min_time)
							{
								time_t days = (wals[j].max_time - wals[j].min_time) / 86400;
								if (days == 0)
									days = 1;
								wal_per_day = wals[j].total_wal / days;
							}
							else if (wals[j].total_wal > 0 && tool_max_time > tool_min_time)
							{
								/* No per-instance time range, use tool total period */
								time_t days = (tool_max_time - tool_min_time) / 86400;
								if (days == 0)
									days = 1;
								wal_per_day = wals[j].total_wal / days;
							}

							if (wal_per_day > 0)
							{
								char wal_str[32];
								format_bytes(wal_per_day, wal_str, sizeof(wal_str));
								printf("      %s: %s/day\n", wals[j].instance_name, wal_str);
							}
						}
					}
				}

				prev_tool = wals[i].tool;
			}
		}
	}

	/* Growth & Efficiency analysis */
	print_growth_efficiency(backups, groups, group_count);

	/* Cleanup */
	free_stat_groups(groups, group_count);
	free_backup_list(backups);

	return EXIT_SUCCESS;
}
