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

typedef struct {
	char *backup_dir;
} StatOptions;

typedef struct {
	BackupTool tool;
	BackupType type;
	char       instance_name[64];
	int        count;
	uint64_t   total_bytes;
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
}

static int
parse_arguments(int argc, char **argv, StatOptions *opts)
{
	int c;
	int option_index = 0;
	bool backup_dir_seen = false;

	static struct option long_options[] = {
		{"backup-dir", required_argument, 0, 'B'},
		{"help",       no_argument,       0, 'h'},
		{0, 0, 0, 0}
	};

	while ((c = getopt_long(argc, argv, "B:h",
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

static void
format_signed_bytes(int64_t bytes, char *buf, size_t size)
{
	const char *sign = (bytes < 0) ? "-" : "+";
	uint64_t abs_bytes = (bytes < 0) ? -bytes : bytes;

	if (abs_bytes >= (uint64_t)1024 * 1024 * 1024)
		snprintf(buf, size, "%s%.1f GB", sign, abs_bytes / (1024.0 * 1024.0 * 1024.0));
	else if (abs_bytes >= (uint64_t)1024 * 1024)
		snprintf(buf, size, "%s%.1f MB", sign, abs_bytes / (1024.0 * 1024.0));
	else if (abs_bytes >= 1024)
		snprintf(buf, size, "%s%.1f KB", sign, abs_bytes / 1024.0);
	else
		snprintf(buf, size, "%s%llu B", sign, (unsigned long long)abs_bytes);
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
	printf("%sGROWTH & EFFICIENCY%s\n", col, rst);
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
			printf("    FULL growth:  N/A (need ≥2 FULL backups)\n");
		}
		else
		{
			int64_t sum_growth = 0, min_growth = 0, max_growth = 0;
			for (int i = 1; i < track->full_count; i++)
			{
				int64_t growth = (int64_t)track->fulls[i].data_bytes -
								 (int64_t)track->fulls[i - 1].data_bytes;
				sum_growth += growth;
				if (i == 1 || growth < min_growth)
					min_growth = growth;
				if (i == 1 || growth > max_growth)
					max_growth = growth;
			}

			char avg_str[32], min_str[32], max_str[32];
			double avg_growth = (double)sum_growth / (track->full_count - 1);
			format_signed_bytes((int64_t)avg_growth, avg_str, sizeof(avg_str));
			format_signed_bytes(min_growth, min_str, sizeof(min_str));
			format_signed_bytes(max_growth, max_str, sizeof(max_str));

			printf("    FULL growth:  avg %s   (min %s .. max %s, %d intervals)\n",
				   avg_str, min_str, max_str, track->full_count - 1);
		}

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
		{
			printf("    (no incremental backups)\n\n");
			continue;
		}

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

	log_init();
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
		return EXIT_NO_BACKUPS_FOUND;
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

		if (b->end_time > b->start_time)
		{
			g->total_duration += (int64_t)(b->end_time - b->start_time);
			g->duration_count++;
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

			printf("  Type      Count    Total Size   Avg Size  Avg Duration   OK%%\n");
			printf("  ──────────────────────────────────────────────────────────────\n");
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

		printf("  %-8s  %5d   %10s   %9s   %11s  %4s\n",
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
	}

	/* Growth & Efficiency analysis */
	print_growth_efficiency(backups, groups, group_count);

	/* Cleanup */
	free_stat_groups(groups, group_count);
	free_backup_list(backups);

	return EXIT_SUCCESS;
}
