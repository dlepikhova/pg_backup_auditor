/*
 * backup_validator.c
 *
 * Backup validation logic
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <stdint.h>

/* Maximum steps when walking a backup chain (cycle / depth guard) */
#define MAX_CHAIN_DEPTH 10000

/* ------------------------------------------------------------------ *
 * Internal helpers
 * ------------------------------------------------------------------ */

static void
add_error(ValidationResult *result, const char *message)
{
	if (result == NULL || message == NULL)
		return;

	result->errors = realloc(result->errors,
							 sizeof(char *) * (result->error_count + 1));
	if (result->errors == NULL)
		return;

	result->errors[result->error_count] = strdup(message);
	result->error_count++;
}

static void
add_warning(ValidationResult *result, const char *message)
{
	if (result == NULL || message == NULL)
		return;

	result->warnings = realloc(result->warnings,
							   sizeof(char *) * (result->warning_count + 1));
	if (result->warnings == NULL)
		return;

	result->warnings[result->warning_count] = strdup(message);
	result->warning_count++;
}

/*
 * merge_result — copy all errors/warnings from src into dst.
 * If prefix is non-NULL, each message is prepended with "<prefix>: ".
 */
static void
merge_result(ValidationResult *dst, const ValidationResult *src,
			 const char *prefix)
{
	char msg[PATH_MAX + 256];
	int  i;

	if (dst == NULL || src == NULL)
		return;

	for (i = 0; i < src->error_count; i++)
	{
		if (prefix != NULL)
			snprintf(msg, sizeof(msg), "%s: %s", prefix, src->errors[i]);
		else
			snprintf(msg, sizeof(msg), "%s", src->errors[i]);
		add_error(dst, msg);
	}
	for (i = 0; i < src->warning_count; i++)
	{
		if (prefix != NULL)
			snprintf(msg, sizeof(msg), "%s: %s", prefix, src->warnings[i]);
		else
			snprintf(msg, sizeof(msg), "%s", src->warnings[i]);
		add_warning(dst, msg);
	}
}

/* ------------------------------------------------------------------ *
 * validate_backup_metadata
 *
 * Checks metadata fields of a single backup (timestamps, LSN range,
 * timeline, version, path existence).  Returns a ValidationResult;
 * the caller is responsible for printing and freeing it.
 * ------------------------------------------------------------------ */
ValidationResult*
validate_backup_metadata(BackupInfo *info)
{
	ValidationResult *result;
	char              msg[PATH_MAX + 100];

	if (info == NULL)
		return NULL;

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;
	result->status = BACKUP_STATUS_OK;

	/* Required fields */
	if (info->backup_id[0] == '\0')
		add_error(result, "Missing backup_id");

	if (info->backup_path[0] == '\0')
		add_error(result, "Missing backup_path");

	if (info->backup_path[0] != '\0' && !is_directory(info->backup_path))
	{
		snprintf(msg, sizeof(msg),
				 "Backup path does not exist: %s", info->backup_path);
		add_error(result, msg);
	}

	/* Timestamps */
	if (info->start_time == 0)
		add_warning(result, "Missing start_time");

	if (info->end_time == 0 && info->status == BACKUP_STATUS_OK)
		add_warning(result, "Missing end_time for completed backup");

	if (info->start_time > 0 && info->end_time > 0 &&
		info->start_time > info->end_time)
	{
		snprintf(msg, sizeof(msg),
				 "Invalid timestamps: start_time (%ld) > end_time (%ld)",
				 (long)info->start_time, (long)info->end_time);
		add_error(result, msg);
	}

	/* LSN range */
	if (info->start_lsn > 0 && info->stop_lsn > 0 &&
		info->start_lsn >= info->stop_lsn)
	{
		snprintf(msg, sizeof(msg),
				 "Invalid LSN range: start_lsn (%" PRIu64 ") >= stop_lsn (%" PRIu64 ")",
				 info->start_lsn, info->stop_lsn);
		add_error(result, msg);
	}

	/* Timeline and version */
	if (info->timeline == 0)
		add_warning(result, "Missing timeline ID");

	if (info->pg_version == 0)
		add_warning(result, "Missing PostgreSQL version");

	if (result->error_count > 0)
		result->status = BACKUP_STATUS_ERROR;
	else if (result->warning_count > 0)
		result->status = BACKUP_STATUS_WARNING;

	return result;
}

/* ------------------------------------------------------------------ *
 * validate_backup_structure
 *
 * Verifies the on-disk layout of a pg_probackup backup.
 * Does NOT read file contents — only checks presence of required paths.
 *
 * ERROR conditions (backup cannot be restored without these):
 *   database/                 — data directory
 *   database/database_map     — OID→dbname map (always written)
 *   database/global/pg_control
 *   database/backup_label     — ARCHIVE mode only (stream=false)
 *
 * WARNING conditions (degraded functionality):
 *   database/PG_VERSION       — missing
 *   backup_content.control    — checksum validation impossible
 *   database/pg_wal/          — missing for STREAM backups (stream=true)
 *
 * Returns NULL for non-pg_probackup tools or empty backup_path.
 * ------------------------------------------------------------------ */
ValidationResult*
validate_backup_structure(BackupInfo *backup)
{
	ValidationResult *result;
	char              path[PATH_MAX];

	if (backup == NULL)
		return NULL;

	if (backup->tool != BACKUP_TOOL_PG_PROBACKUP)
		return NULL;

	if (backup->backup_path[0] == '\0')
		return NULL;

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;
	result->status = BACKUP_STATUS_OK;

	/* database/ */
	path_join(path, sizeof(path), backup->backup_path, "database");
	if (!is_directory(path))
	{
		add_error(result, "Missing database/ directory");
	}
	else
	{
		/* database/database_map */
		path_join(path, sizeof(path), backup->backup_path, "database");
		path_join(path, sizeof(path), path, "database_map");
		if (!file_exists(path))
			add_error(result, "Missing database/database_map");

		/* database/global/pg_control */
		{
			char ctrl[PATH_MAX];
			path_join(ctrl, sizeof(ctrl), backup->backup_path, "database");
			path_join(ctrl, sizeof(ctrl), ctrl, "global");
			path_join(ctrl, sizeof(ctrl), ctrl, "pg_control");
			if (!file_exists(ctrl))
				add_error(result, "Missing database/global/pg_control");
		}

		/* database/backup_label (ARCHIVE mode only) */
		if (!backup->wal_stream)
		{
			path_join(path, sizeof(path), backup->backup_path, "database");
			path_join(path, sizeof(path), path, "backup_label");
			if (!file_exists(path))
				add_error(result,
						  "Missing database/backup_label "
						  "(required for archive-mode backup)");
		}

		/* database/PG_VERSION — only present in FULL backups;
		 * pg_probackup skips unchanged files in DELTA/PAGE/PTRACK */
		if (backup->type == BACKUP_TYPE_FULL)
		{
			path_join(path, sizeof(path), backup->backup_path, "database");
			path_join(path, sizeof(path), path, "PG_VERSION");
			if (!file_exists(path))
				add_warning(result, "Missing database/PG_VERSION");
		}

		/* database/pg_wal/ (STREAM mode) */
		if (backup->wal_stream)
		{
			char wal_dir[PATH_MAX];
			path_join(wal_dir, sizeof(wal_dir), backup->backup_path, "database");
			path_join(wal_dir, sizeof(wal_dir), wal_dir, "pg_wal");
			if (!is_directory(wal_dir))
				add_warning(result,
							"Missing database/pg_wal/ "
							"(expected for stream backup)");
		}

		/*
		 * tablespace_map is absent when no non-default tablespaces exist,
		 * so its absence is not an error.
		 *
		 * TODO: parse tablespace_map entries and verify that the referenced
		 * pg_tblspc/<OID> subdirectories exist inside the backup.
		 * TODO: parse database_map JSON and verify base/<OID>/ directories.
		 */
	}

	/* backup_content.control */
	path_join(path, sizeof(path),
			  backup->backup_path, "backup_content.control");
	if (!file_exists(path))
		add_warning(result,
					"Missing backup_content.control "
					"(checksum validation not possible)");

	if (result->error_count > 0)
		result->status = BACKUP_STATUS_ERROR;
	else if (result->warning_count > 0)
		result->status = BACKUP_STATUS_WARNING;

	return result;
}

/* ------------------------------------------------------------------ *
 * validate_single_backup
 *
 * Runs all applicable validation checks for a single backup according
 * to the requested level.  This is the per-backup unit of validation;
 * validate_backup_chain() calls it for every link in the chain.
 *
 * BASIC:    file structure (validate_backup_structure)
 * STANDARD: + metadata (validate_backup_metadata)
 * CHECKSUMS:+ file-level CRC32C (check_backup_checksums)
 *            + WAL availability (check_wal_availability)
 *            + WAL header validation (check_wal_headers)
 *            WAL source for pg_probackup:
 *              - ARCHIVE mode: uses the wal_info passed by the caller
 *              - STREAM mode:  scans database/pg_wal/ inside the backup
 * FULL:     + WAL timeline history (check_wal_timeline)
 *
 * Returns a ValidationResult that the caller must free.
 * ------------------------------------------------------------------ */
ValidationResult*
validate_single_backup(BackupInfo *backup, WALArchiveInfo *wal_info,
					   ValidationLevel level)
{
	ValidationResult *result;
	WALArchiveInfo   *effective_wal = NULL;
	WALArchiveInfo   *stream_wal   = NULL;

	if (backup == NULL)
		return NULL;

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;
	result->status = BACKUP_STATUS_OK;

	/* Level 1: structure */
	if (level >= VALIDATION_LEVEL_BASIC)
	{
		ValidationResult *sr = validate_backup_structure(backup);
		if (sr != NULL)
		{
			merge_result(result, sr, NULL);
			free_validation_result(sr);
		}
	}

	/* Level 2: metadata */
	if (level >= VALIDATION_LEVEL_STANDARD)
	{
		ValidationResult *mr = validate_backup_metadata(backup);
		if (mr != NULL)
		{
			merge_result(result, mr, NULL);
			free_validation_result(mr);
		}
	}

	/* Level 3+: checksums + WAL */
	if (level >= VALIDATION_LEVEL_CHECKSUMS)
	{
		/* File-level checksums */
		ValidationResult *cr = check_backup_checksums(backup);
		if (cr != NULL)
		{
			merge_result(result, cr, NULL);
			free_validation_result(cr);
		}

		/* Determine WAL source */
		if (backup->start_lsn > 0 && backup->stop_lsn > 0)
		{
			if (!backup->wal_stream)
			{
				/* Archive mode: use external wal_info */
				effective_wal = wal_info;
			}
			else if (backup->tool == BACKUP_TOOL_PG_PROBACKUP &&
					 backup->backup_path[0] != '\0')
			{
				/* Stream mode: WAL is embedded in database/pg_wal/.
				 * Always prefer embedded WAL — the external archive may
				 * be missing segments that the stream backup carries
				 * internally, and the backup is self-sufficient. */
				char pg_wal_path[PATH_MAX];
				path_join(pg_wal_path, sizeof(pg_wal_path),
						  backup->backup_path, "database");
				path_join(pg_wal_path, sizeof(pg_wal_path),
						  pg_wal_path, "pg_wal");
				if (is_directory(pg_wal_path))
				{
					stream_wal    = scan_wal_archive(pg_wal_path);
					effective_wal = stream_wal;
				}
				else
					effective_wal = wal_info;  /* fallback if pg_wal/ absent */
			}
			else if (wal_info != NULL)
			{
				effective_wal = wal_info;
			}

			if (effective_wal != NULL)
			{
				ValidationResult *wr, *hr;

				wr = check_wal_availability(backup, effective_wal);
				if (wr != NULL)
				{
					merge_result(result, wr, NULL);
					free_validation_result(wr);
				}

				hr = check_wal_headers(backup, effective_wal);
				if (hr != NULL)
				{
					merge_result(result, hr, NULL);
					free_validation_result(hr);
				}
			}
			else if (backup->wal_stream)
			{
				add_warning(result,
							"WAL not verified: stream backup with no "
							"accessible WAL (database/pg_wal/ missing or "
							"empty, and no --wal-archive provided)");
			}
		}
	}

	/* Level 4: timeline history */
	if (level >= VALIDATION_LEVEL_FULL && effective_wal != NULL)
	{
		ValidationResult *tr = check_wal_timeline(backup, effective_wal);
		if (tr != NULL)
		{
			merge_result(result, tr, NULL);
			free_validation_result(tr);
		}
	}

	if (stream_wal != NULL)
		free_wal_archive_info(stream_wal);

	if (result->error_count > 0)
		result->status = BACKUP_STATUS_ERROR;
	else if (result->warning_count > 0)
		result->status = BACKUP_STATUS_WARNING;

	return result;
}

/* ------------------------------------------------------------------ *
 * validate_backup_chain
 *
 * Top-level validation entry point.  Validates the entire chain that
 * is required to restore the given backup:
 *
 *   - For any backup: runs validate_single_backup() on the backup itself.
 *   - For pg_probackup FULL: additionally checks that parent_backup_id
 *     is empty (FULL is the chain root — it has no parent).
 *   - For pg_probackup DELTA/PAGE/PTRACK: walks up to the root FULL
 *     backup and runs validate_single_backup() on every ancestor.
 *     Ancestor errors/warnings are prefixed with "Parent <ID>: " so
 *     the caller can clearly identify which link in the chain failed.
 *
 * LSN sanity (DELTA/PAGE/PTRACK): each child's start_lsn must not be
 * earlier than its parent's start_lsn.  Violations are reported as
 * warnings rather than errors, since LSN fields can be absent (0).
 *
 * Cycle / depth guard: aborts after MAX_CHAIN_DEPTH steps and reports
 * an error.  MAX_CHAIN_DEPTH is 10 000, which is large enough for any
 * realistic backup catalog.
 *
 * Non-pg_probackup backups are validated as a chain of one (just the
 * backup itself, no connectivity checks).
 * ------------------------------------------------------------------ */
ValidationResult*
validate_backup_chain(BackupInfo *backup, BackupInfo *all_backups,
					  WALArchiveInfo *wal_info, ValidationLevel level)
{
	ValidationResult *result;
	char              msg[256];

	if (backup == NULL)
		return NULL;

	/* Step 1: validate the target backup itself */
	result = validate_single_backup(backup, wal_info, level);
	if (result == NULL)
		return NULL;

	/* Step 2: pg_probackup chain checks */
	if (backup->tool != BACKUP_TOOL_PG_PROBACKUP)
		goto done;

	if (backup->type == BACKUP_TYPE_FULL)
	{
		/* FULL must have no parent */
		if (backup->parent_backup_id[0] != '\0')
		{
			snprintf(msg, sizeof(msg),
					 "FULL backup has unexpected parent_backup_id: %s",
					 backup->parent_backup_id);
			add_error(result, msg);
		}
		goto done;
	}

	if (backup->type != BACKUP_TYPE_DELTA &&
		backup->type != BACKUP_TYPE_PAGE  &&
		backup->type != BACKUP_TYPE_PTRACK)
		goto done;

	/* Step 3: walk up the chain for incremental backups */
	if (backup->parent_backup_id[0] == '\0')
	{
		add_error(result, "Incremental backup has no parent_backup_id");
		goto done;
	}

	{
		BackupInfo *current = backup;
		int         depth   = 0;

		while (current->type != BACKUP_TYPE_FULL)
		{
			const char *pid    = current->parent_backup_id;
			BackupInfo *parent = NULL;

			if (pid[0] == '\0')
			{
				snprintf(msg, sizeof(msg),
						 "Broken chain at %s: missing parent_backup_id",
						 current->backup_id);
				add_error(result, msg);
				break;
			}

			/* Find parent in catalog */
			for (BackupInfo *b = all_backups; b != NULL; b = b->next)
			{
				if (strcmp(b->backup_id, pid) == 0)
				{
					parent = b;
					break;
				}
			}

			if (parent == NULL)
			{
				snprintf(msg, sizeof(msg),
						 "Parent backup %s not found in catalog", pid);
				add_error(result, msg);
				break;
			}

			/* Check parent status before validating */
			if (parent->status == BACKUP_STATUS_ERROR   ||
				parent->status == BACKUP_STATUS_CORRUPT ||
				parent->status == BACKUP_STATUS_ORPHAN)
			{
				snprintf(msg, sizeof(msg),
						 "Parent backup %s has status %s — chain is not "
						 "restorable",
						 parent->backup_id,
						 backup_status_to_string(parent->status));
				add_error(result, msg);
				break;
			}

			/* Validate the parent backup */
			{
				ValidationResult *pr = validate_single_backup(parent, wal_info,
															  level);
				if (pr != NULL)
				{
					if (pr->error_count > 0)
					{
						snprintf(msg, sizeof(msg),
								 "Ancestor %s has validation errors — "
								 "chain recovery may fail",
								 parent->backup_id);
						add_warning(result, msg);
						free_validation_result(pr);
						break;  /* no point walking further */
					}
					else if (pr->warning_count > 0)
					{
						snprintf(msg, sizeof(msg),
								 "Ancestor %s has validation warnings",
								 parent->backup_id);
						add_warning(result, msg);
					}
					free_validation_result(pr);
				}
			}

			/* LSN sanity: child must not start before its parent */
			if (parent->start_lsn > 0 && current->start_lsn > 0 &&
				current->start_lsn < parent->start_lsn)
			{
				snprintf(msg, sizeof(msg),
						 "LSN regression: backup %s start_lsn is before "
						 "parent %s start_lsn",
						 current->backup_id, parent->backup_id);
				add_warning(result, msg);
			}

			current = parent;
			depth++;

			if (depth >= MAX_CHAIN_DEPTH)
			{
				add_error(result,
						  "Circular reference or excessively deep "
						  "backup chain");
				break;
			}
		}
	}

done:
	if (result->error_count > 0)
		result->status = BACKUP_STATUS_ERROR;
	else if (result->warning_count > 0)
		result->status = BACKUP_STATUS_WARNING;
	else
		result->status = BACKUP_STATUS_OK;

	return result;
}

/* ------------------------------------------------------------------ *
 * check_retention_policy — stub
 * ------------------------------------------------------------------ */
ValidationResult*
check_retention_policy(BackupInfo *backups, int retention_days,
					   int retention_weekly)
{
	(void) backups;
	(void) retention_days;
	(void) retention_weekly;
	return NULL;
}

/* ------------------------------------------------------------------ *
 * free_validation_result
 * ------------------------------------------------------------------ */
void
free_validation_result(ValidationResult *result)
{
	int i;

	if (result == NULL)
		return;

	if (result->errors != NULL)
	{
		for (i = 0; i < result->error_count; i++)
			free(result->errors[i]);
		free(result->errors);
	}

	if (result->warnings != NULL)
	{
		for (i = 0; i < result->warning_count; i++)
			free(result->warnings[i]);
		free(result->warnings);
	}

	free(result);
}

/* ------------------------------------------------------------------ *
 * json_get_string — used by check_backup_checksums
 *
 * Extracts a value from a JSON line: {"key":"value", ...} or
 * {"key":123, ...}.
 * ------------------------------------------------------------------ */
static bool
json_get_string(const char *json, const char *key, char *out, size_t outsize)
{
	char        search[128];
	const char *p, *end;
	size_t      len;

	/* Quoted value */
	snprintf(search, sizeof(search), "\"%s\":\"", key);
	p = strstr(json, search);
	if (p != NULL)
	{
		p  += strlen(search);
		end = strchr(p, '"');
		if (end == NULL) return false;
		len = (size_t)(end - p);
		if (len >= outsize) len = outsize - 1;
		memcpy(out, p, len);
		out[len] = '\0';
		return true;
	}

	/* Bare numeric value */
	snprintf(search, sizeof(search), "\"%s\":", key);
	p = strstr(json, search);
	if (p != NULL)
	{
		p  += strlen(search);
		end = p;
		while (*end && *end != ',' && *end != '}')
			end++;
		len = (size_t)(end - p);
		if (len >= outsize) len = outsize - 1;
		memcpy(out, p, len);
		out[len] = '\0';
		return true;
	}

	return false;
}

/* ------------------------------------------------------------------ *
 * check_backup_checksums
 *
 * Parses backup_content.control (one JSON object per line) and verifies
 * for each regular file entry:
 *   1. File exists in the backup's database/ directory
 *   2. Size matches the stored "size" field
 *   3. CRC32C matches the stored "crc" field (uncompressed files only)
 *
 * Returns NULL if backup_content.control is absent (tool unsupported).
 * ------------------------------------------------------------------ */
ValidationResult*
check_backup_checksums(BackupInfo *backup)
{
	char             content_path[PATH_MAX];
	char             db_dir[PATH_MAX];
	char             file_path[PATH_MAX];
	FILE            *fp;
	char             line[8192];
	ValidationResult *result;
	char             msg[PATH_MAX + 128];
	int              checked            = 0;
	int              skipped_compressed = 0;

	if (backup == NULL || backup->backup_path[0] == '\0')
		return NULL;

	path_join(content_path, sizeof(content_path),
			  backup->backup_path, "backup_content.control");

	if (!file_exists(content_path))
		return NULL;

	fp = fopen(content_path, "r");
	if (fp == NULL)
		return NULL;

	path_join(db_dir, sizeof(db_dir), backup->backup_path, "database");

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
	{
		fclose(fp);
		return NULL;
	}
	result->status = BACKUP_STATUS_OK;

	while (fgets(line, sizeof(line), fp) != NULL)
	{
		char kind[16]           = {0};
		char rel_path[PATH_MAX] = {0};
		char size_str[32]       = {0};
		char crc_str[32]        = {0};
		char compress[32]       = {0};

		if (!json_get_string(line, "kind", kind, sizeof(kind)))
			continue;
		if (strcmp(kind, "reg") != 0)
			continue;

		if (!json_get_string(line, "path", rel_path, sizeof(rel_path)))
			continue;
		if (!json_get_string(line, "size", size_str, sizeof(size_str)))
			continue;
		if (!json_get_string(line, "crc", crc_str, sizeof(crc_str)))
			continue;
		json_get_string(line, "compress_alg", compress, sizeof(compress));

		path_join(file_path, sizeof(file_path), db_dir, rel_path);

		/* Zero-size files are not stored physically by pg_probackup */
		if (strcmp(size_str, "0") == 0)
			continue;

		/* Incremental backups: skip files that belong to a parent */
		if (backup->type != BACKUP_TYPE_FULL && !file_exists(file_path))
			continue;

		if (!file_exists(file_path))
		{
			snprintf(msg, sizeof(msg), "Missing file: %s", rel_path);
			add_error(result, msg);
			continue;
		}

		/* Size check */
		{
			off_t stored_size = (off_t) strtoull(size_str, NULL, 10);
			off_t actual_size = get_file_size(file_path);
			if (actual_size != stored_size)
			{
				snprintf(msg, sizeof(msg),
						 "Size mismatch for %s: stored=%lld, actual=%lld",
						 rel_path,
						 (long long)stored_size, (long long)actual_size);
				add_error(result, msg);
				continue;
			}
		}

		/* CRC32C check (uncompressed files only).
		 * global/pg_control is modified by pg_probackup after writing
		 * backup_content.control, so its stored CRC never matches. */
		if (strcmp(rel_path, "global/pg_control") == 0)
		{
			checked++;
			continue;
		}

		if (strcmp(compress, "none") == 0 || compress[0] == '\0')
		{
			uint32_t stored_crc = (uint32_t) strtoul(crc_str, NULL, 10);
			uint32_t actual_crc = 0;

			if (!compute_file_crc32c(file_path, &actual_crc))
			{
				snprintf(msg, sizeof(msg),
						 "Cannot read file for CRC check: %s", rel_path);
				add_error(result, msg);
				continue;
			}

			if (actual_crc != stored_crc)
			{
				snprintf(msg, sizeof(msg),
						 "CRC32C mismatch for %s: "
						 "stored=0x%08X, actual=0x%08X",
						 rel_path, stored_crc, actual_crc);
				add_error(result, msg);
			}
		}
		else
		{
			skipped_compressed++;
		}

		checked++;
	}

	fclose(fp);

	log_debug("Checksum check: %d files verified, %d compressed skipped, "
			  "%d errors",
			  checked, skipped_compressed, result->error_count);

	return result;
}
