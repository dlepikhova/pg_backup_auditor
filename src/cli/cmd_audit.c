/*
 * cmd_audit.c
 *
 * Implementation of 'audit' command
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
#include "backup_chain.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <getopt.h>
#include <sys/statvfs.h>

/* Command-line options */
typedef struct {
	char *backup_dir;
	char *wal_archive;
	bool detect_size_small;
} AuditOptions;

static void
init_options(AuditOptions *opts)
{
	opts->backup_dir  = NULL;
	opts->wal_archive = NULL;
	opts->detect_size_small = false;
}

static int
parse_arguments(int argc, char **argv, AuditOptions *opts)
{
	int c;
	int option_index = 0;
	bool backup_dir_seen  = false;
	bool wal_archive_seen = false;

	static struct option long_options[] = {
		{"backup-dir",           required_argument, 0, 'B'},
		{"wal-archive",          required_argument, 0, 'w'},
		{"detect-size-small",    no_argument,       0, 's'},
		{"help",                 no_argument,       0, 'h'},
		{0, 0, 0, 0}
	};

	while ((c = getopt_long(argc, argv, "B:w:sh",
							long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case 'B':
				if (!parse_string_option("--backup-dir", optarg, &opts->backup_dir, &backup_dir_seen))
					return EXIT_INVALID_ARGUMENTS;
				break;
			case 'w':
				if (!parse_string_option("--wal-archive", optarg, &opts->wal_archive, &wal_archive_seen))
					return EXIT_INVALID_ARGUMENTS;
				break;
			case 's':
				opts->detect_size_small = true;
				break;
			case 'h':
				print_audit_usage();
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
validate_options(const AuditOptions *opts)
{
	if (!validate_required_option(opts->backup_dir, "--backup-dir"))
		return EXIT_INVALID_ARGUMENTS;

	if (!is_directory(opts->backup_dir))
	{
		fprintf(stderr, "Error: Backup directory does not exist: %s\n", opts->backup_dir);
		return EXIT_GENERAL_ERROR;
	}

	if (opts->wal_archive != NULL && !is_directory(opts->wal_archive))
	{
		fprintf(stderr, "Error: WAL archive directory does not exist: %s\n", opts->wal_archive);
		return EXIT_GENERAL_ERROR;
	}

	return EXIT_SUCCESS;
}


/* ------------------------------------------------------------------ *
 * Formatting helpers
 * ------------------------------------------------------------------ */

static void
format_duration(time_t seconds, char *buf, size_t size)
{
	if (seconds < 0)
		seconds = 0;
	long s     = (long)seconds;
	long days  = s / 86400; s %= 86400;
	long hours = s / 3600;  s %= 3600;
	long mins  = s / 60;

	if (days > 0)
		snprintf(buf, size, "%ldd %ldh %ldm", days, hours, mins);
	else if (hours > 0)
		snprintf(buf, size, "%ldh %ldm", hours, mins);
	else
		snprintf(buf, size, "%ldm", mins);
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

/* ------------------------------------------------------------------ *
 * Anomaly detection
 * ------------------------------------------------------------------ */

typedef struct {
	char backup_id[64];
	const char *anomaly_type;  /* "size_large", "size_small", "duration_long", "duration_short" */
	double value;
	double avg;
	double multiplier;  /* value / avg */
} AnomalyRecord;

typedef struct {
	AnomalyRecord *items;
	int count;
	int capacity;
} AnomalyList;

/* Per-backup-type statistics (per tool AND type) */
typedef struct {
	BackupTool tool;
	BackupType type;
	int count;
	uint64_t total_size;
	time_t total_duration;
	double avg_size;
	double avg_duration;
	double stddev_size;
} BackupTypeStats;

/* ------------------------------------------------------------------ *
 * Chain status assessment
 * ------------------------------------------------------------------ */

typedef enum {
	CHAIN_STATUS_OK,
	CHAIN_STATUS_DEGRADED,
	CHAIN_STATUS_BROKEN
} ChainStatus;

/* Calculate statistics for each backup tool+type combination (only OK/WARNING backups) */
BackupTypeStats*
calculate_backup_stats(BackupInfo *backups, int *out_count)
{
	BackupTypeStats *stats = malloc(30 * sizeof(BackupTypeStats));  /* Max 30 combinations */
	if (stats == NULL)
		return NULL;

	int stats_count = 0;
	memset(stats, 0, 30 * sizeof(BackupTypeStats));

	/* First pass: group by (tool, type) and sum */
	for (BackupInfo *b = backups; b != NULL; b = b->next)
	{
		/* Skip failed/incomplete backups */
		if (b->status != BACKUP_STATUS_OK && b->status != BACKUP_STATUS_WARNING)
			continue;

		/* Skip if no data */
		if (b->data_bytes == 0 || b->start_time == 0 || b->end_time == 0)
			continue;

		/* Skip backups with non-positive duration (end <= start). Otherwise
		 * zero-duration entries pull the average toward 0, and entries where
		 * end < start (broken timestamps) feed a negative value into the
		 * sum, corrupting the baseline for every real backup. */
		if (b->end_time <= b->start_time)
			continue;

		BackupTypeStats *st = NULL;
		for (int i = 0; i < stats_count; i++)
		{
			if (stats[i].tool == b->tool && stats[i].type == b->type)
			{
				st = &stats[i];
				break;
			}
		}

		if (st == NULL)
		{
			if (stats_count >= 30)
				break;  /* Too many combinations */
			st = &stats[stats_count++];
			st->tool = b->tool;
			st->type = b->type;
			st->count = 0;
			st->total_size = 0;
			st->total_duration = 0;
		}

		st->count++;
		st->total_size += b->data_bytes + b->wal_bytes;
		st->total_duration += (b->end_time - b->start_time);
	}

	/* Calculate averages */
	for (int i = 0; i < stats_count; i++)
	{
		if (stats[i].count > 0)
		{
			stats[i].avg_size = (double)stats[i].total_size / stats[i].count;
			stats[i].avg_duration = (double)stats[i].total_duration / stats[i].count;
		}
	}

	*out_count = stats_count;
	return stats;
}

/* Detect anomalies in backup sizes and durations */
AnomalyList*
detect_anomalies(BackupInfo *backups, BackupTypeStats *stats, int stats_count, bool detect_size_small)
{
	AnomalyList *anomalies = malloc(sizeof(AnomalyList));
	if (anomalies == NULL)
		return NULL;

	anomalies->items = malloc(100 * sizeof(AnomalyRecord));  /* Max 100 anomalies */
	anomalies->count = 0;
	anomalies->capacity = 100;

	if (anomalies->items == NULL)
	{
		free(anomalies);
		return NULL;
	}

	const double SIZE_THRESHOLD = 2.0;     /* 2x average = anomaly */
	const double DURATION_THRESHOLD = 2.0; /* 2x average = anomaly */

	for (BackupInfo *b = backups; b != NULL; b = b->next)
	{
		/* Skip failed/incomplete backups */
		if (b->status != BACKUP_STATUS_OK && b->status != BACKUP_STATUS_WARNING)
			continue;

		/* Skip if no data */
		if (b->data_bytes == 0 || b->start_time == 0 || b->end_time == 0)
			continue;

		/* Skip backups with non-positive duration (end <= start). The
		 * duration math below would otherwise produce a 0 or negative
		 * multiplier and report a meaningless "Nx faster" anomaly. */
		if (b->end_time <= b->start_time)
			continue;

		/* Find stats for this (tool, type) combination */
		BackupTypeStats *st = NULL;
		for (int i = 0; i < stats_count; i++)
		{
			if (stats[i].tool == b->tool && stats[i].type == b->type && stats[i].count > 1)
			{
				st = &stats[i];
				break;
			}
		}

		if (st == NULL)
			continue;  /* Not enough data for this combination */

		uint64_t size = b->data_bytes + b->wal_bytes;
		time_t duration = b->end_time - b->start_time;

		/* Check for size anomalies */
		double size_multiplier = size / st->avg_size;
		if (size_multiplier > SIZE_THRESHOLD)
		{
			AnomalyRecord *rec = &anomalies->items[anomalies->count++];
			str_copy(rec->backup_id, b->backup_id, sizeof(rec->backup_id));
			rec->anomaly_type = "size_large";
			rec->value = size;
			rec->avg = st->avg_size;
			rec->multiplier = size_multiplier;

			if (anomalies->count >= anomalies->capacity)
				break;
		}
		else if (detect_size_small && size_multiplier < (1.0 / SIZE_THRESHOLD) && size_multiplier > 0)
		{
			AnomalyRecord *rec = &anomalies->items[anomalies->count++];
			str_copy(rec->backup_id, b->backup_id, sizeof(rec->backup_id));
			rec->anomaly_type = "size_small";
			rec->value = size;
			rec->avg = st->avg_size;
			rec->multiplier = 1.0 / size_multiplier;

			if (anomalies->count >= anomalies->capacity)
				break;
		}

		/* Check for duration anomalies */
		double duration_multiplier = duration / st->avg_duration;
		if (duration_multiplier > DURATION_THRESHOLD)
		{
			AnomalyRecord *rec = &anomalies->items[anomalies->count++];
			str_copy(rec->backup_id, b->backup_id, sizeof(rec->backup_id));
			rec->anomaly_type = "duration_long";
			rec->value = duration;
			rec->avg = st->avg_duration;
			rec->multiplier = duration_multiplier;

			if (anomalies->count >= anomalies->capacity)
				break;
		}
		else if (duration_multiplier < (1.0 / DURATION_THRESHOLD) && duration_multiplier > 0)
		{
			AnomalyRecord *rec = &anomalies->items[anomalies->count++];
			str_copy(rec->backup_id, b->backup_id, sizeof(rec->backup_id));
			rec->anomaly_type = "duration_short";
			rec->value = duration;
			rec->avg = st->avg_duration;
			rec->multiplier = 1.0 / duration_multiplier;

			if (anomalies->count >= anomalies->capacity)
				break;
		}
	}

	return anomalies;
}

/* Find the latest WAL segment after a given LSN in the archive */
static bool
get_latest_wal_lsn(const WALArchiveInfo *wal_info, XLogRecPtr after_lsn,
				   XLogRecPtr *out_lsn)
{
	if (wal_info == NULL || wal_info->segment_count == 0)
		return false;

	/* Segments are sorted by timeline, log_id, seg_id (highest first after sort) */
	/* We need to find the last segment that is >= after_lsn */
	XLogRecPtr max_lsn = 0;
	bool found = false;

	for (int i = 0; i < wal_info->segment_count; i++)
	{
		WALSegmentName *seg = &wal_info->segments[i];
		XLogRecPtr seg_lsn = ((XLogRecPtr)seg->log_id << 32) | (seg->seg_id * 16 * 1024 * 1024);

		if (seg_lsn > after_lsn)
		{
			if (seg_lsn > max_lsn)
			{
				max_lsn = seg_lsn;
				found = true;
			}
		}
	}

	if (found)
	{
		*out_lsn = max_lsn;
		return true;
	}
	return false;
}

/* Calculate how many WAL segments can be safely removed (older than oldest backup) */
static uint64_t
calculate_deletable_wal_size(const WALArchiveInfo *wal_info, XLogRecPtr oldest_lsn)
{
	if (wal_info == NULL || wal_info->segment_count == 0)
		return 0;

	uint64_t deletable_size = 0;

	/* All segments before oldest_lsn can be deleted */
	for (int i = 0; i < wal_info->segment_count; i++)
	{
		WALSegmentName *seg = &wal_info->segments[i];
		XLogRecPtr seg_lsn = ((XLogRecPtr)seg->log_id << 32) | (seg->seg_id * 16 * 1024 * 1024);

		if (seg_lsn < oldest_lsn)
		{
			deletable_size += 16 * 1024 * 1024;  /* 16 MB per segment */
		}
	}

	return deletable_size;
}

/* Check if WAL archive covers from backup stop_lsn to the latest segment */
static bool
is_wal_continuous_after_lsn(const WALArchiveInfo *wal_info, XLogRecPtr from_lsn)
{
	if (wal_info == NULL || wal_info->segment_count == 0)
		return false;

	/* Check if there's a segment starting at or shortly after from_lsn */
	/* A segment at LSN X covers [X, X + 16MB) */
	for (int i = 0; i < wal_info->segment_count; i++)
	{
		WALSegmentName *seg = &wal_info->segments[i];
		XLogRecPtr seg_lsn = ((XLogRecPtr)seg->log_id << 32) | (seg->seg_id * 16 * 1024 * 1024);

		/* If we find a segment that covers from_lsn or is the next one */
		if (seg_lsn <= from_lsn && seg_lsn + (16UL * 1024 * 1024) > from_lsn)
			return true;  /* Continuous from backup stop point */

		if (seg_lsn > from_lsn)
		{
			/* Check if there's a gap - next segment should start at or before
			 * the point where previous segment would end */
			if (i > 0)
			{
				WALSegmentName *prev = &wal_info->segments[i - 1];
				XLogRecPtr prev_lsn = ((XLogRecPtr)prev->log_id << 32) | (prev->seg_id * 16 * 1024 * 1024);
				if (prev_lsn + (16UL * 1024 * 1024) >= seg_lsn)
					return true;  /* No gap */
			}
			return false;  /* Gap detected */
		}
	}

	return false;  /* No segment covers the range */
}

static ChainStatus
assess_chain_status(const BackupChain *chain)
{
	if (chain->root == NULL)
		return CHAIN_STATUS_BROKEN;  /* orphaned group: no FULL backup */

	if (chain->root->status == BACKUP_STATUS_ERROR ||
		chain->root->status == BACKUP_STATUS_CORRUPT)
		return CHAIN_STATUS_BROKEN;

	for (int i = 0; i < chain->count; i++)
	{
		BackupInfo *b = chain->members[i];
		if (b->status == BACKUP_STATUS_RUNNING)
			return CHAIN_STATUS_DEGRADED;
		if (b->status == BACKUP_STATUS_ERROR ||
			b->status == BACKUP_STATUS_CORRUPT)
			return CHAIN_STATUS_DEGRADED;
	}

	return CHAIN_STATUS_OK;
}

static const char *
chain_status_label(ChainStatus s)
{
	switch (s)
	{
		case CHAIN_STATUS_OK:       return "OK";
		case CHAIN_STATUS_DEGRADED: return "DEGRADED";
		case CHAIN_STATUS_BROKEN:   return "BROKEN";
	}
	return "UNKNOWN";
}

static const char *
chain_status_color(ChainStatus s)
{
	switch (s)
	{
		case CHAIN_STATUS_OK:       return COLOR_GREEN;
		case CHAIN_STATUS_DEGRADED: return COLOR_YELLOW;
		case CHAIN_STATUS_BROKEN:   return COLOR_RED;
	}
	return "";
}

/* ------------------------------------------------------------------ *
 * Section: one FULL chain
 * ------------------------------------------------------------------ */

static ChainStatus
print_chain_audit(const BackupChain *chain, int chain_num, const WALArchiveInfo *wal_info)
{
	ChainStatus status = assess_chain_status(chain);

	/* Chain header */
	{
		char date_str[32] = "N/A";
		if (chain->root->start_time > 0)
			strftime(date_str, sizeof(date_str), "%Y-%m-%d",
					 localtime(&chain->root->start_time));

		int incr_count = chain->count - 1;
		printf("Chain %d: %s  %s  %s",
			   chain_num,
			   chain->root->backup_id,
			   backup_tool_to_string(chain->root->tool),
			   date_str);
		if (incr_count > 0)
			printf("  (+%d incremental%s)", incr_count, incr_count == 1 ? "" : "s");
		printf("\n");
	}

	/* Status line */
	{
		const char *col = use_color ? chain_status_color(status) : "";
		const char *rst = use_color ? COLOR_RESET : "";
		printf("  Status:                  %s[%s]%s\n",
			   col, chain_status_label(status), rst);
	}

	/* WAL mode: use per-backup wal_mode; "mixed" if not all the same */
	{
		const char *first = chain->count > 0 ? chain->members[0]->wal_mode : "";
		bool all_same = true;
		for (int i = 1; i < chain->count; i++)
		{
			if (strcmp(chain->members[i]->wal_mode, first) != 0)
			{
				all_same = false;
				break;
			}
		}
		printf("  WAL Mode:                %s\n",
			   (first[0] == '\0') ? "-" : (all_same ? first : "mixed"));
	}

	/* Find oldest/latest completed backup in chain for recovery points */
	time_t     oldest_time = 0;
	time_t     latest_time = 0;
	XLogRecPtr oldest_lsn  = 0;
	XLogRecPtr latest_lsn  = 0;
	time_t     last_end    = 0;  /* newest end_time, for RPO gap */

	for (int i = 0; i < chain->count; i++)
	{
		BackupInfo *b = chain->members[i];

		/* Only count completed backups towards recovery points */
		if (b->status != BACKUP_STATUS_OK && b->status != BACKUP_STATUS_WARNING)
			continue;

		if (oldest_time == 0 || b->start_time < oldest_time)
		{
			oldest_time = b->start_time;
			oldest_lsn  = b->start_lsn;
		}
		if (b->end_time > latest_time)
		{
			latest_time = b->end_time;
			latest_lsn  = b->stop_lsn;
		}
		if (b->end_time > last_end)
			last_end = b->end_time;
	}

	/* Recovery points */
	if (oldest_time > 0)
	{
		char ts[32], lsn_str[32];

		format_timestamp(oldest_time, ts, sizeof(ts));
		format_lsn(oldest_lsn, lsn_str, sizeof(lsn_str));
		printf("  Oldest recovery point:   %s  (%s) [from backup]\n", ts, lsn_str);

		format_timestamp(latest_time, ts, sizeof(ts));
		format_lsn(latest_lsn, lsn_str, sizeof(lsn_str));
		printf("  Latest recovery point:   %s  (%s) [from backup]\n", ts, lsn_str);

		/* Check if WAL archive extends recovery window */
		if (wal_info != NULL && wal_info->segment_count > 0)
		{
			XLogRecPtr wal_lsn = 0;
			if (get_latest_wal_lsn(wal_info, latest_lsn, &wal_lsn))
			{
				format_lsn(wal_lsn, lsn_str, sizeof(lsn_str));

				/* Check if WAL is continuous from backup stop_lsn */
				if (is_wal_continuous_after_lsn(wal_info, latest_lsn))
				{
					printf("  Latest recovery point:   (with continuous WAL) (%s) [with WAL archive]\n", lsn_str);
				}
				else
				{
					const char *col = use_color ? COLOR_YELLOW : "";
					const char *rst = use_color ? COLOR_RESET : "";
					printf("  Latest recovery point:   (with WAL - %sGAP DETECTED%s) (%s)\n",
						   col, rst, lsn_str);
				}
			}
		}

		/* RPO gap: time elapsed since last completed backup */
		time_t now = time(NULL);
		char   dur[32];
		format_duration((time_t)difftime(now, last_end), dur, sizeof(dur));
		printf("  RPO gap:                 %s\n", dur);
	}
	else
	{
		printf("  Oldest recovery point:   N/A\n");
		printf("  Latest recovery point:   N/A\n");
		printf("  RPO gap:                 N/A\n");
	}

	return status;
}

/* ------------------------------------------------------------------ *
 * Section: anomalies
 * ------------------------------------------------------------------ */

static void
print_anomalies_section(const AnomalyList *anomalies)
{
	if (anomalies == NULL || anomalies->count == 0)
		return;

	const char *col = use_color ? COLOR_CYAN : "";
	const char *rst = use_color ? COLOR_RESET : "";
	printf("%sAnomalies%s\n", col, rst);
	printf("  Detected unusual backup patterns:\n");

	for (int i = 0; i < anomalies->count; i++)
	{
		const AnomalyRecord *rec = &anomalies->items[i];
		char val_str[32], avg_str[32];

		if (strcmp(rec->anomaly_type, "size_large") == 0)
		{
			format_bytes(rec->value, val_str, sizeof(val_str));
			format_bytes(rec->avg, avg_str, sizeof(avg_str));
			printf("  - %s: size %s (%.1fx larger than avg %s)\n",
				   rec->backup_id, val_str, rec->multiplier, avg_str);
		}
		else if (strcmp(rec->anomaly_type, "size_small") == 0)
		{
			format_bytes(rec->value, val_str, sizeof(val_str));
			format_bytes(rec->avg, avg_str, sizeof(avg_str));
			printf("  - %s: size %s (%.1fx smaller than avg %s)\n",
				   rec->backup_id, val_str, rec->multiplier, avg_str);
		}
		else if (strcmp(rec->anomaly_type, "duration_long") == 0)
		{
			char dur_str[32], avg_dur_str[32];
			format_duration((time_t)rec->value, dur_str, sizeof(dur_str));
			format_duration((time_t)rec->avg, avg_dur_str, sizeof(avg_dur_str));
			printf("  - %s: took %s (%.1fx longer than avg %s)\n",
				   rec->backup_id, dur_str, rec->multiplier, avg_dur_str);
		}
		else if (strcmp(rec->anomaly_type, "duration_short") == 0)
		{
			char dur_str[32], avg_dur_str[32];
			format_duration((time_t)rec->value, dur_str, sizeof(dur_str));
			format_duration((time_t)rec->avg, avg_dur_str, sizeof(avg_dur_str));
			printf("  - %s: took %s (%.1fx faster than avg %s)\n",
				   rec->backup_id, dur_str, rec->multiplier, avg_dur_str);
		}
	}
}

/* ------------------------------------------------------------------ *
 * Section: orphaned backups
 * ------------------------------------------------------------------ */

static int
print_orphans_section(const BackupChain *orphan_chain)
{
	if (orphan_chain == NULL || orphan_chain->count == 0)
		return 0;

	const char *col = use_color ? COLOR_CYAN : "";
	const char *rst = use_color ? COLOR_RESET : "";
	printf("%sOrphaned Backups (%d)%s\n", col, orphan_chain->count, rst);
	printf("  ");
	for (int i = 0; i < orphan_chain->count; i++)
	{
		if (i > 0)
			printf(", ");
		printf("%s", orphan_chain->members[i]->backup_id);
	}
	printf("\n");
	printf("  These backups cannot be used for restore without their parent FULL backup\n");

	return orphan_chain->count;
}

/* ------------------------------------------------------------------ *
 * Section: WAL archive
 * ------------------------------------------------------------------ */

static bool
print_wal_section(const char *wal_archive_dir, WALArchiveInfo *wal_info,
				  BackupInfo *backups)
{
	bool wal_ok = true;

	const char *col = use_color ? COLOR_CYAN : "";
	const char *rst = use_color ? COLOR_RESET : "";
	printf("%sWAL%s\n", col, rst);

	if (wal_archive_dir == NULL || wal_info == NULL)
	{
		printf("  No WAL archive provided (use --wal-archive to enable)\n");
		return true;
	}

	printf("  Archive:    %s\n", wal_archive_dir);
	printf("  Segments:   %d\n", wal_info->segment_count);

	uint64_t wal_size = get_directory_size(wal_archive_dir);
	char     size_str[32];
	format_bytes(wal_size, size_str, sizeof(size_str));
	printf("  Size:       %s\n", size_str);

	/* WAL restore chain coverage */
	ValidationResult *chain_result = check_wal_restore_chain(backups, wal_info);
	if (chain_result != NULL)
	{
		if (chain_result->error_count > 0)
		{
			printf("  Coverage:   %s[INCOMPLETE]%s WAL gaps detected:\n",
				   use_color ? COLOR_RED : "", use_color ? COLOR_RESET : "");
			for (int i = 0; i < chain_result->error_count; i++)
				printf("              %s\n", chain_result->errors[i]);
			wal_ok = false;
		}
		else
		{
			printf("  Coverage:   %s[OK]%s Continuous WAL available for all backups\n",
				   use_color ? COLOR_GREEN : "", use_color ? COLOR_RESET : "");
		}
		free_validation_result(chain_result);
	}

	/* WAL cleanup recommendations */
	if (backups != NULL && wal_info->segment_count > 0)
	{
		/* Find oldest backup's start_lsn to calculate cleanable segments */
		XLogRecPtr oldest_lsn = 0;
		for (BackupInfo *b = backups; b != NULL; b = b->next)
		{
			if (b->start_lsn > 0)
			{
				if (oldest_lsn == 0 || b->start_lsn < oldest_lsn)
					oldest_lsn = b->start_lsn;
			}
		}

		if (oldest_lsn > 0)
		{
			uint64_t deletable_size = calculate_deletable_wal_size(wal_info, oldest_lsn);
			if (deletable_size > 0)
			{
				format_bytes(deletable_size, size_str, sizeof(size_str));
				printf("  Cleanup:    %s of WAL segments can be safely removed (older than oldest backup)\n",
					   size_str);
			}
		}
	}

	return wal_ok;
}

/* ------------------------------------------------------------------ *
 * Section: storage
 * ------------------------------------------------------------------ */

static void
print_storage_section(const char *backup_dir, BackupInfo *backups)
{
	const char *col = use_color ? COLOR_CYAN : "";
	const char *rst = use_color ? COLOR_RESET : "";
	printf("%sSTORAGE%s\n", col, rst);

	/* Sum backup sizes from metadata */
	uint64_t total_backup_bytes = 0;
	int      running_count      = 0;

	for (BackupInfo *b = backups; b != NULL; b = b->next)
	{
		total_backup_bytes += b->data_bytes + b->wal_bytes;
		if (b->status == BACKUP_STATUS_RUNNING)
			running_count++;
	}

	char size_str[32];
	format_bytes(total_backup_bytes, size_str, sizeof(size_str));
	printf("  Total backup size:   %s\n", size_str);

	/* Disk usage via statvfs */
	struct statvfs vfs;
	if (statvfs(backup_dir, &vfs) == 0)
	{
		uint64_t disk_total = (uint64_t)vfs.f_blocks * vfs.f_frsize;
		uint64_t disk_free  = (uint64_t)vfs.f_bavail * vfs.f_frsize;
		double   pct_free   = disk_total > 0 ? 100.0 * disk_free / disk_total : 0.0;

		char total_str[32], free_str[32];
		format_bytes(disk_total, total_str, sizeof(total_str));
		format_bytes(disk_free,  free_str,  sizeof(free_str));

		printf("  Disk total:          %s\n", total_str);
		printf("  Disk free:           %s (%.1f%%)\n", free_str, pct_free);
	}
	else
	{
		printf("  Disk usage:          unavailable\n");
	}

	/* RUNNING backups */
	if (running_count > 0)
		printf("  %s[WARNING]%s %d backup%s currently RUNNING\n",
			   use_color ? COLOR_YELLOW : "", use_color ? COLOR_RESET : "",
			   running_count, running_count == 1 ? " is" : "s are");
	else
		printf("  Backups RUNNING:     none\n");
}

/* ------------------------------------------------------------------ *
 * Main entry point
 * ------------------------------------------------------------------ */

int
cmd_audit_main(int argc, char **argv)
{
	AuditOptions    opts;
	BackupInfo     *backups         = NULL;
	WALArchiveInfo *wal_info        = NULL;
	char           *wal_path_alloc  = NULL;  /* auto-detected path, must be freed */
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

	/* Scan WAL archive — explicit or auto-detected */
	const char *wal_archive_display = opts.wal_archive;  /* path shown in output */

	if (opts.wal_archive != NULL)
	{
		log_info("Scanning WAL archive: %s", opts.wal_archive);
		wal_info = scan_wal_archive(opts.wal_archive);
		if (wal_info == NULL)
			log_warning("Failed to scan WAL archive: %s", opts.wal_archive);
		else
			log_info("Found %d WAL segments in archive", wal_info->segment_count);
	}
	else
	{
		/* Auto-detect WAL archive for archive-mode backups */
		for (BackupInfo *b = backups; b != NULL; b = b->next)
		{
			if (b->wal_stream || b->start_lsn == 0)
				continue;
			BackupAdapter *adapter = get_adapter_for_tool(b->tool);
			if (adapter == NULL || adapter->get_wal_archive_path == NULL)
				continue;
			char *wal_path = adapter->get_wal_archive_path(
				b->backup_path, b->instance_name);
			if (wal_path == NULL)
				continue;
			log_info("Auto-detected WAL archive: %s", wal_path);
			wal_info = scan_wal_archive(wal_path);
			if (wal_info != NULL)
			{
				wal_path_alloc    = wal_path;
				wal_archive_display = wal_path_alloc;
				log_info("Found %d WAL segments in archive", wal_info->segment_count);
			}
			else
			{
				free(wal_path);
			}
			break;
		}
	}

	/* Build chains */
	int          nchains = 0;
	BackupChain *chains  = backup_chain_build(backups, &nchains);
	if (chains == NULL)
	{
		fprintf(stderr, "Error: Failed to build backup chains\n");
		free_backup_list(backups);
		if (wal_info != NULL)
			free_wal_archive_info(wal_info);
		return EXIT_GENERAL_ERROR;
	}

	/* Header */
	{
		time_t now = time(NULL);
		char   now_str[32];
		format_timestamp(now, now_str, sizeof(now_str));

		const char *title_col = use_color ? COLOR_CYAN : "";
		const char *rst = use_color ? COLOR_RESET : "";

		printf("%sBackup Audit%s\n", title_col, rst);
		char resolved_dir[PATH_MAX];
		const char *display_dir = opts.backup_dir;
		if (realpath(opts.backup_dir, resolved_dir) != NULL)
			display_dir = resolved_dir;
		printf("Directory:  %s\n", display_dir);
		printf("Time:       %s\n", now_str);
	}

	printf("\n");

	/* Separate FULL chains from the orphaned bucket (always last if non-empty) */
	int              full_count    = nchains;
	const BackupChain *orphan_bucket = NULL;

	if (nchains > 0 && chains[nchains - 1].root == NULL)
	{
		full_count    = nchains - 1;
		orphan_bucket = &chains[nchains - 1];
	}

	/* Track overall status */
	bool has_broken   = (full_count == 0);
	bool has_degraded = false;
	bool wal_ok       = true;

	/* CHAINS section */
	{
		const char *col = use_color ? COLOR_CYAN : "";
		const char *rst = use_color ? COLOR_RESET : "";
		printf("%sCHAINS%s\n", col, rst);
	}

	for (int ci = 0; ci < full_count; ci++)
	{
		ChainStatus cs = print_chain_audit(&chains[ci], ci + 1, wal_info);
		if (cs == CHAIN_STATUS_BROKEN)   has_broken   = true;
		if (cs == CHAIN_STATUS_DEGRADED) has_degraded = true;
	}

	printf("\n────────────────────────────────────────────────────────────────\n");

	/* Orphaned backups */
	if (orphan_bucket != NULL && orphan_bucket->count > 0)
	{
		print_orphans_section(orphan_bucket);
		printf("────────────────────────────────────────────────────────────────\n");
		has_degraded = true;
	}

	/* Anomaly detection */
	int stats_count = 0;
	BackupTypeStats *stats = calculate_backup_stats(backups, &stats_count);
	AnomalyList *anomalies = NULL;
	if (stats != NULL && stats_count > 0)
	{
		anomalies = detect_anomalies(backups, stats, stats_count, opts.detect_size_small);
	}
	if (anomalies != NULL && anomalies->count > 0)
	{
		print_anomalies_section(anomalies);
		printf("────────────────────────────────────────────────────────────────\n");
		has_degraded = true;
	}

	/* WAL section */
	wal_ok = print_wal_section(wal_archive_display, wal_info, backups);
	if (!wal_ok)
		has_degraded = true;

	printf("────────────────────────────────────────────────────────────────\n");

	/* STORAGE section */
	print_storage_section(opts.backup_dir, backups);

	printf("\n────────────────────────────────────────────────────────────────\n");

	/* Verdict */
	if (has_broken)
	{
		const char *col = use_color ? COLOR_RED    : "";
		const char *rst = use_color ? COLOR_RESET  : "";
		printf("%s✗ Verdict: CRITICAL%s\n", col, rst);
		if (full_count == 0)
			printf("  No complete backup chains found.\n");
		else
			printf("  One or more backup chains are BROKEN and cannot be restored.\n");
	}
	else if (has_degraded)
	{
		const char *col = use_color ? COLOR_YELLOW : "";
		const char *rst = use_color ? COLOR_RESET  : "";
		printf("%s⚠ Verdict: WARNING%s\n", col, rst);
		printf("  Backup chains are restorable but issues were detected.\n");
	}
	else
	{
		const char *col = use_color ? COLOR_GREEN  : "";
		const char *rst = use_color ? COLOR_RESET  : "";
		printf("%s✓ Verdict: OK%s\n", col, rst);
		printf("  All backup chains are healthy and restorable.\n");
	}

	/* Cleanup */
	if (anomalies != NULL)
	{
		free(anomalies->items);
		free(anomalies);
	}
	if (stats != NULL)
		free(stats);
	backup_chain_free(chains, nchains);
	free_backup_list(backups);
	if (wal_info != NULL)
		free_wal_archive_info(wal_info);
	if (wal_path_alloc != NULL)
		free(wal_path_alloc);

	return (has_broken || has_degraded) ? EXIT_VALIDATION_FAILED : EXIT_SUCCESS;
}
