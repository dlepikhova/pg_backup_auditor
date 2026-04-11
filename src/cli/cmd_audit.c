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
} AuditOptions;

static void
init_options(AuditOptions *opts)
{
	opts->backup_dir  = NULL;
	opts->wal_archive = NULL;
}

static int
parse_arguments(int argc, char **argv, AuditOptions *opts)
{
	int c;
	int option_index = 0;
	bool backup_dir_seen  = false;
	bool wal_archive_seen = false;

	static struct option long_options[] = {
		{"backup-dir",  required_argument, 0, 'B'},
		{"wal-archive", required_argument, 0, 'w'},
		{"help",        no_argument,       0, 'h'},
		{0, 0, 0, 0}
	};

	while ((c = getopt_long(argc, argv, "B:w:h",
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
			case 'w':
				if (check_duplicate_option(wal_archive_seen, "--wal-archive"))
					return EXIT_INVALID_ARGUMENTS;
				opts->wal_archive = optarg;
				wal_archive_seen = true;
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
 * Chain grouping helpers (mirrors cmd_check.c)
 * ------------------------------------------------------------------ */

typedef struct {
	BackupInfo  *root;      /* Root FULL backup; NULL = orphaned group */
	BackupInfo **members;   /* Sorted by start_time, oldest first */
	int          count;
	int          capacity;
} BackupChain;

static BackupInfo *
find_backup_in_list(BackupInfo *list, const char *id)
{
	for (BackupInfo *b = list; b != NULL; b = b->next)
		if (strcmp(b->backup_id, id) == 0)
			return b;
	return NULL;
}

static BackupInfo *
find_chain_root(BackupInfo *backup, BackupInfo *all_backups)
{
	BackupInfo *cur = backup;
	for (int depth = 0; depth < 1000; depth++)
	{
		if (cur->type == BACKUP_TYPE_FULL)
			return cur;
		if (cur->parent_backup_id[0] == '\0')
			return NULL;
		cur = find_backup_in_list(all_backups, cur->parent_backup_id);
		if (cur == NULL)
			return NULL;
	}
	return NULL;  /* cycle guard */
}

static int
compare_backup_by_time(const void *a, const void *b)
{
	time_t ta = (*(const BackupInfo **)a)->start_time;
	time_t tb = (*(const BackupInfo **)b)->start_time;
	return (ta > tb) - (ta < tb);
}

static bool
chain_append(BackupChain *chain, BackupInfo *backup)
{
	if (chain->count == chain->capacity)
	{
		int nc = chain->capacity ? chain->capacity * 2 : 8;
		BackupInfo **nm = realloc(chain->members, nc * sizeof(*nm));
		if (nm == NULL)
			return false;
		chain->members = nm;
		chain->capacity = nc;
	}
	chain->members[chain->count++] = backup;
	return true;
}

static BackupChain *
build_chains(BackupInfo *all_backups, int *nchains)
{
	int full_count = 0;
	for (BackupInfo *b = all_backups; b != NULL; b = b->next)
		if (b->type == BACKUP_TYPE_FULL)
			full_count++;

	BackupChain *chains = calloc(full_count + 1, sizeof(BackupChain));
	if (chains == NULL)
		return NULL;

	int ci = 0;
	for (BackupInfo *b = all_backups; b != NULL; b = b->next)
	{
		if (b->type != BACKUP_TYPE_FULL)
			continue;
		chains[ci].root = b;
		chain_append(&chains[ci], b);
		ci++;
	}

	int orphan = full_count;
	for (BackupInfo *b = all_backups; b != NULL; b = b->next)
	{
		if (b->type == BACKUP_TYPE_FULL)
			continue;

		BackupInfo *root   = find_chain_root(b, all_backups);
		int         target = orphan;

		if (root != NULL)
		{
			for (int i = 0; i < full_count; i++)
			{
				if (chains[i].root == root)
				{
					target = i;
					break;
				}
			}
		}
		chain_append(&chains[target], b);
	}

	int total = full_count;
	if (chains[orphan].count > 0)
		total++;

	for (int i = 0; i < total; i++)
	{
		if (chains[i].count > 1)
			qsort(chains[i].members, chains[i].count,
				  sizeof(BackupInfo *), compare_backup_by_time);
	}

	*nchains = total;
	return chains;
}

static void
free_chains(BackupChain *chains, int count)
{
	if (chains == NULL)
		return;
	for (int i = 0; i < count; i++)
		free(chains[i].members);
	free(chains);
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
 * Chain status assessment
 * ------------------------------------------------------------------ */

typedef enum {
	CHAIN_STATUS_OK,
	CHAIN_STATUS_DEGRADED,
	CHAIN_STATUS_BROKEN
} ChainStatus;

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
print_chain_audit(const BackupChain *chain, int chain_num)
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
		printf("  Oldest recovery point:   %s  (%s)\n", ts, lsn_str);

		format_timestamp(latest_time, ts, sizeof(ts));
		format_lsn(latest_lsn, lsn_str, sizeof(lsn_str));
		printf("  Latest recovery point:   %s  (%s)\n", ts, lsn_str);

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

	printf("\n");
	return status;
}

/* ------------------------------------------------------------------ *
 * Section: orphaned backups
 * ------------------------------------------------------------------ */

static int
print_orphans_section(const BackupChain *orphan_chain)
{
	if (orphan_chain == NULL || orphan_chain->count == 0)
		return 0;

	printf("Orphaned Backups (%d)\n", orphan_chain->count);
	printf("  ");
	for (int i = 0; i < orphan_chain->count; i++)
	{
		if (i > 0)
			printf(", ");
		printf("%s", orphan_chain->members[i]->backup_id);
	}
	printf("\n");
	printf("  %s[WARNING]%s These backups cannot be used for restore "
		   "without their parent FULL backup\n",
		   use_color ? COLOR_YELLOW : "", use_color ? COLOR_RESET : "");
	printf("\n");

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

	printf("WAL\n");
	printf("----------------------------------------------------\n");

	if (wal_archive_dir == NULL || wal_info == NULL)
	{
		printf("  No WAL archive provided (use --wal-archive to enable)\n\n");
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

	printf("\n");
	return wal_ok;
}

/* ------------------------------------------------------------------ *
 * Section: storage
 * ------------------------------------------------------------------ */

static void
print_storage_section(const char *backup_dir, BackupInfo *backups)
{
	printf("STORAGE\n");
	printf("----------------------------------------------------\n");

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

	printf("\n");
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
	BackupChain *chains  = build_chains(backups, &nchains);
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

		printf("====================================================\n");
		printf("Backup Audit\n");
		printf("====================================================\n");
		char resolved_dir[PATH_MAX];
		const char *display_dir = opts.backup_dir;
		if (realpath(opts.backup_dir, resolved_dir) != NULL)
			display_dir = resolved_dir;
		printf("Directory:  %s\n", display_dir);
		printf("Time:       %s\n", now_str);
		printf("====================================================\n\n");
	}

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
	printf("CHAINS\n");
	printf("----------------------------------------------------\n\n");

	for (int ci = 0; ci < full_count; ci++)
	{
		ChainStatus cs = print_chain_audit(&chains[ci], ci + 1);
		if (cs == CHAIN_STATUS_BROKEN)   has_broken   = true;
		if (cs == CHAIN_STATUS_DEGRADED) has_degraded = true;
	}

	/* Orphaned backups */
	if (orphan_bucket != NULL && orphan_bucket->count > 0)
	{
		print_orphans_section(orphan_bucket);
		has_degraded = true;
	}

	/* WAL section */
	wal_ok = print_wal_section(wal_archive_display, wal_info, backups);
	if (!wal_ok)
		has_degraded = true;

	/* STORAGE section */
	print_storage_section(opts.backup_dir, backups);

	/* Verdict */
	printf("====================================================\n");
	if (has_broken)
	{
		const char *col = use_color ? COLOR_RED    : "";
		const char *rst = use_color ? COLOR_RESET  : "";
		printf("%sVerdict: CRITICAL%s\n", col, rst);
		if (full_count == 0)
			printf("  No complete backup chains found.\n");
		else
			printf("  One or more backup chains are BROKEN and cannot be used for restore.\n");
	}
	else if (has_degraded)
	{
		const char *col = use_color ? COLOR_YELLOW : "";
		const char *rst = use_color ? COLOR_RESET  : "";
		printf("%sVerdict: WARNING%s\n", col, rst);
		printf("  Backup chains are restorable but issues were detected.\n");
	}
	else
	{
		const char *col = use_color ? COLOR_GREEN  : "";
		const char *rst = use_color ? COLOR_RESET  : "";
		printf("%sVerdict: OK%s\n", col, rst);
		printf("  All backup chains are healthy and restorable.\n");
	}
	printf("====================================================\n");

	/* Cleanup */
	free_chains(chains, nchains);
	free_backup_list(backups);
	if (wal_info != NULL)
		free_wal_archive_info(wal_info);
	if (wal_path_alloc != NULL)
		free(wal_path_alloc);

	return (has_broken || has_degraded) ? EXIT_VALIDATION_FAILED : EXIT_SUCCESS;
}
