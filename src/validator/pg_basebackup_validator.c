/*
 * pg_basebackup_validator.c
 *
 * Structure validation and embedded WAL access for pg_basebackup backups.
 * Implements the validate_structure and get_embedded_wal adapter hooks.
 *
 * Supported formats:
 *   plain — standard directory layout (base/, global/, pg_wal/)
 *   tar   — base.tar[.gz|.bz2|.xz|.lz4] + pg_wal.tar[.*]
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
#include "sha256.h"
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <stdio.h>
#include <inttypes.h>
#include <strings.h>

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
 * has_tar_file — check if the backup directory contains a file whose
 * name starts with the given prefix (e.g. "base.tar" or "pg_wal.tar").
 */
static bool
has_tar_file(const char *backup_path, const char *prefix)
{
	DIR           *dir;
	struct dirent *entry;
	bool           found = false;
	size_t         plen  = strlen(prefix);

	dir = opendir(backup_path);
	if (dir == NULL)
		return false;

	while ((entry = readdir(dir)) != NULL)
	{
		if (strncmp(entry->d_name, prefix, plen) == 0)
		{
			found = true;
			break;
		}
	}

	closedir(dir);
	return found;
}

/*
 * find_tar_file — find the full path of the first file whose name starts
 * with the given prefix inside backup_path.  Writes into out (size outsz).
 * Returns true if found.
 */
static bool
find_tar_file(const char *backup_path, const char *prefix,
			  char *out, size_t outsz)
{
	DIR           *dir;
	struct dirent *entry;
	bool           found = false;
	size_t         plen  = strlen(prefix);

	dir = opendir(backup_path);
	if (dir == NULL)
		return false;

	while ((entry = readdir(dir)) != NULL)
	{
		if (strncmp(entry->d_name, prefix, plen) == 0)
		{
			path_join(out, outsz, backup_path, entry->d_name);
			found = true;
			break;
		}
	}

	closedir(dir);
	return found;
}

/* ------------------------------------------------------------------ *
 * pg_basebackup_validate_structure
 *
 * Verifies the on-disk layout of a pg_basebackup backup.
 *
 * Plain format ERROR conditions:
 *   base/               — data directory
 *   global/pg_control   — needed for recovery
 *   backup_label or backup_manifest — at least one must exist
 *
 * Plain format WARNING conditions:
 *   pg_wal/             — missing for stream backups (wal_stream=true)
 *
 * Tar format ERROR conditions:
 *   base.tar*           — main data tarball must exist
 *
 * Tar format WARNING conditions:
 *   pg_wal.tar*         — missing for stream backups (wal_stream=true)
 * ------------------------------------------------------------------ */
ValidationResult*
pg_basebackup_validate_structure(BackupInfo *backup)
{
	ValidationResult *result;
	char              path[PATH_MAX];
	bool              is_tar;

	if (backup == NULL || backup->backup_path[0] == '\0')
		return NULL;

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;
	result->status = BACKUP_STATUS_OK;

	is_tar = has_tar_file(backup->backup_path, "base.tar");

	if (is_tar)
	{
		/* Tar format */
		if (!has_tar_file(backup->backup_path, "base.tar"))
			add_error(result, "Missing base.tar* (main data tarball)");

		if (backup->wal_stream && !has_tar_file(backup->backup_path, "pg_wal.tar"))
			add_warning(result,
						"Missing pg_wal.tar* (expected for stream backup)");
	}
	else
	{
		/* Plain format */
		path_join(path, sizeof(path), backup->backup_path, "base");
		if (!is_directory(path))
			add_error(result, "Missing base/ directory");

		path_join(path, sizeof(path), backup->backup_path, "global");
		path_join(path, sizeof(path), path, "pg_control");
		if (!file_exists(path))
			add_error(result, "Missing global/pg_control");

		/* backup_label or backup_manifest must exist */
		{
			char label[PATH_MAX], manifest[PATH_MAX];
			path_join(label,    sizeof(label),    backup->backup_path, "backup_label");
			path_join(manifest, sizeof(manifest), backup->backup_path, "backup_manifest");
			if (!file_exists(label) && !file_exists(manifest))
				add_error(result,
						  "Missing backup_label and backup_manifest "
						  "(at least one is required)");
		}

		/* PG_VERSION — always present in a valid plain backup */
		path_join(path, sizeof(path), backup->backup_path, "PG_VERSION");
		if (!file_exists(path))
			add_warning(result, "Missing PG_VERSION");

		if (backup->wal_stream)
		{
			path_join(path, sizeof(path), backup->backup_path, "pg_wal");
			if (!is_directory(path))
				add_warning(result,
							"Missing pg_wal/ (expected for stream backup)");
		}
	}

	if (result->error_count > 0)
		result->status = BACKUP_STATUS_ERROR;
	else if (result->warning_count > 0)
		result->status = BACKUP_STATUS_WARNING;

	return result;
}

/* ------------------------------------------------------------------ *
 * pg_basebackup_get_embedded_wal
 *
 * For plain format stream backups: scan pg_wal/ directory.
 * For tar format stream backups: scan the directory for pg_wal.tar*
 *   and pass it to scan_wal_archive (which handles tar scanning).
 * Archive-mode backups return NULL.
 * ------------------------------------------------------------------ */
WALArchiveInfo*
pg_basebackup_get_embedded_wal(BackupInfo *backup)
{
	char path[PATH_MAX];

	if (backup == NULL || backup->backup_path[0] == '\0')
		return NULL;

	if (!backup->wal_stream)
		return NULL;

	/* Tar format: pg_wal.tar* */
	if (has_tar_file(backup->backup_path, "base.tar"))
	{
		if (find_tar_file(backup->backup_path, "pg_wal.tar", path, sizeof(path)))
			return scan_wal_archive(path);
		return NULL;
	}

	/* Plain format: pg_wal/ directory */
	path_join(path, sizeof(path), backup->backup_path, "pg_wal");
	if (!is_directory(path))
		return NULL;

	return scan_wal_archive(path);
}

/* ------------------------------------------------------------------ *
 * check_manifest_checksums
 *
 * Validates pg_basebackup's backup_manifest (PostgreSQL 13+).
 *
 * For each file entry with "Checksum-Algorithm": "SHA256":
 *   - verifies the file exists at backup_path/<Path>
 *   - computes SHA256 and compares against "Checksum" field
 *
 * Also verifies the manifest's own integrity checksum
 * ("Manifest-Checksum" covers all manifest bytes before that key).
 *
 * Returns NULL when backup_manifest does not exist (not applicable).
 * Tar-format backups: only the manifest self-checksum is verified
 * (individual files inside the tar are not accessible without unpacking).
 * ------------------------------------------------------------------ */

/*
 * Helper: extract a JSON string value following "Key": "VALUE"
 * within a single line.  Writes at most outsz-1 chars into out.
 * Returns true if found.
 */
static bool
json_extract_string(const char *line, const char *key,
					char *out, size_t outsz)
{
	const char *p;
	const char *q;
	size_t      len;

	p = strstr(line, key);
	if (p == NULL)
		return false;

	p += strlen(key);

	/* skip whitespace and ':' */
	while (*p == ' ' || *p == '\t' || *p == ':')
		p++;

	if (*p != '"')
		return false;
	p++;   /* skip opening quote */

	q = strchr(p, '"');
	if (q == NULL)
		return false;

	len = (size_t)(q - p);
	if (len >= outsz)
		len = outsz - 1;

	memcpy(out, p, len);
	out[len] = '\0';
	return true;
}

/*
 * Helper: extract a JSON integer value following "Key": NUMBER
 * Returns true if found.
 */
static bool
json_extract_uint64(const char *line, const char *key, uint64_t *out)
{
	const char *p;

	p = strstr(line, key);
	if (p == NULL)
		return false;

	p += strlen(key);
	while (*p == ' ' || *p == '\t' || *p == ':')
		p++;

	if (*p < '0' || *p > '9')
		return false;

	*out = (uint64_t)strtoull(p, NULL, 10);
	return true;
}

ValidationResult*
check_manifest_checksums(BackupInfo *backup)
{
	ValidationResult *result;
	char              manifest_path[PATH_MAX];
	FILE             *fp;
	char              line[4096];
	bool              is_tar;
	int               checked  = 0;
	int               errors   = 0;

	/* Per-entry state */
	char     entry_path[PATH_MAX];
	char     entry_algo[32];
	char     entry_cksum[SHA256_HEX_LENGTH + 2];
	uint64_t entry_size;
	bool     have_path, have_algo, have_cksum, have_size;

	/* Manifest self-checksum state */
	SHA256Ctx manifest_ctx;
	uint8_t   manifest_digest[SHA256_DIGEST_LENGTH];
	char      manifest_hex[SHA256_HEX_LENGTH + 1];
	char      stored_manifest_cksum[SHA256_HEX_LENGTH + 2];
	bool      have_manifest_cksum = false;
	long      manifest_cksum_offset = 0;   /* byte offset of "Manifest-Checksum" line */
	char      msg[PATH_MAX + 128];

	if (backup == NULL || backup->backup_path[0] == '\0')
		return NULL;

	path_join(manifest_path, sizeof(manifest_path),
			  backup->backup_path, "backup_manifest");

	if (!file_exists(manifest_path))
		return NULL;   /* not applicable — no manifest */

	result = calloc(1, sizeof(ValidationResult));
	if (result == NULL)
		return NULL;
	result->status = BACKUP_STATUS_OK;

	is_tar = has_tar_file(backup->backup_path, "base.tar");

	/*
	 * First pass: find the byte offset where "Manifest-Checksum" begins
	 * so we can hash only the preceding bytes.
	 */
	fp = fopen(manifest_path, "r");
	if (fp == NULL)
	{
		add_error(result, "Cannot open backup_manifest");
		result->status = BACKUP_STATUS_ERROR;
		return result;
	}

	manifest_cksum_offset = 0;
	while (fgets(line, sizeof(line), fp) != NULL)
	{
		if (strstr(line, "\"Manifest-Checksum\"") != NULL)
		{
			/* Record where this line starts */
			if (json_extract_string(line, "\"Manifest-Checksum\"",
									stored_manifest_cksum,
									sizeof(stored_manifest_cksum)))
				have_manifest_cksum = true;
			break;
		}
		manifest_cksum_offset = ftell(fp);
	}
	fclose(fp);

	/*
	 * Verify manifest self-checksum: hash the first manifest_cksum_offset
	 * bytes of the file and compare against stored_manifest_cksum.
	 */
	if (have_manifest_cksum && manifest_cksum_offset > 0)
	{
		uint8_t  buf[8192];
		size_t   remaining = (size_t)manifest_cksum_offset;
		size_t   nread;

		fp = fopen(manifest_path, "rb");
		if (fp != NULL)
		{
			sha256_init(&manifest_ctx);
			while (remaining > 0)
			{
				size_t want = remaining < sizeof(buf) ? remaining : sizeof(buf);
				nread = fread(buf, 1, want, fp);
				if (nread == 0) break;
				sha256_update(&manifest_ctx, buf, nread);
				remaining -= nread;
			}
			fclose(fp);

			sha256_final(&manifest_ctx, manifest_digest);
			sha256_to_hex(manifest_digest, manifest_hex);

			if (strcasecmp(manifest_hex, stored_manifest_cksum) != 0)
			{
				snprintf(msg, sizeof(msg),
						 "backup_manifest self-checksum mismatch "
						 "(expected %s, got %s)",
						 stored_manifest_cksum, manifest_hex);
				add_error(result, msg);
				errors++;
			}
		}
	}
	else if (!have_manifest_cksum)
	{
		add_warning(result,
					"backup_manifest has no Manifest-Checksum "
					"(PostgreSQL < 13 or truncated manifest)");
	}

	/*
	 * For tar format we cannot check individual files without unpacking,
	 * so stop here — the manifest self-checksum above is sufficient.
	 */
	if (is_tar)
		goto done;

	/*
	 * Second pass: validate per-file checksums (SHA256 and CRC32C).
	 */
	fp = fopen(manifest_path, "r");
	if (fp == NULL)
		goto done;

	entry_path[0]  = '\0';
	entry_algo[0]  = '\0';
	entry_cksum[0] = '\0';
	entry_size     = 0;
	have_path = have_algo = have_cksum = have_size = false;

	while (fgets(line, sizeof(line), fp) != NULL)
	{
		/* Stop at Manifest-Checksum — no more file entries after it */
		if (strstr(line, "\"Manifest-Checksum\"") != NULL)
			break;

		/* Accumulate fields for the current entry */
		if (!have_path)
			have_path = json_extract_string(line, "\"Path\"",
											entry_path, sizeof(entry_path));
		if (!have_algo)
			have_algo = json_extract_string(line, "\"Checksum-Algorithm\"",
											entry_algo, sizeof(entry_algo));
		if (!have_cksum)
			have_cksum = json_extract_string(line, "\"Checksum\"",
											 entry_cksum, sizeof(entry_cksum));
		if (!have_size)
			have_size = json_extract_uint64(line, "\"Size\"", &entry_size);

		/*
		 * Once we have Path + Algorithm + Checksum (or NONE), validate.
		 * Entry is complete when we have at minimum: path + algo + cksum,
		 * OR path + algo=NONE + size.
		 */
		if (!have_path || !have_algo)
			continue;
		if (!have_cksum && !(strcasecmp(entry_algo, "NONE") == 0 && have_size))
			continue;

		{
			char file_path[PATH_MAX];
			bool skip = false;

			path_join(file_path, sizeof(file_path),
					  backup->backup_path, entry_path);

			/* Skip pg_wal entries — covered by WAL validation */
			if (strncmp(entry_path, "pg_wal/", 7) == 0 ||
				strcmp(entry_path, "pg_wal") == 0)
				skip = true;

			if (!skip)
			{
				if (!file_exists(file_path))
				{
					/* Missing files only matter if size > 0 */
					if (!have_size || entry_size > 0)
					{
						snprintf(msg, sizeof(msg),
								 "Missing file: %s", entry_path);
						add_error(result, msg);
						errors++;
					}
				}
				else if (strcasecmp(entry_algo, "SHA256") == 0)
				{
					uint8_t digest[SHA256_DIGEST_LENGTH];
					char    hex[SHA256_HEX_LENGTH + 1];

					checked++;
					if (sha256_file(file_path, digest))
					{
						sha256_to_hex(digest, hex);
						if (strcasecmp(hex, entry_cksum) != 0)
						{
							snprintf(msg, sizeof(msg),
									 "SHA256 mismatch: %s "
									 "(expected %.16s…, got %.16s…)",
									 entry_path, entry_cksum, hex);
							add_error(result, msg);
							errors++;
						}
					}
					else
					{
						snprintf(msg, sizeof(msg),
								 "Cannot read file for checksum: %s",
								 entry_path);
						add_warning(result, msg);
					}
				}
				else if (strcasecmp(entry_algo, "CRC32C") == 0)
				{
					uint32_t crc = 0;
					uint32_t expected;

					checked++;

					/*
					 * PostgreSQL stores CRC32C as little-endian bytes,
					 * each byte printed as 2 hex digits (pg_checksum_final).
					 * e.g. value 0xFB8EEC93 → "93EC8EFB".
					 * Parse byte-by-byte and reconstruct as LE uint32.
					 */
					{
						uint8_t b[4] = {0, 0, 0, 0};
						size_t  hlen = strlen(entry_cksum);

						if (hlen == 8)
						{
							for (int bi = 0; bi < 4; bi++)
							{
								char tmp[3] = { entry_cksum[bi*2],
												entry_cksum[bi*2+1], '\0' };
								b[bi] = (uint8_t)strtoul(tmp, NULL, 16);
							}
						}
						expected = (uint32_t)b[0]
								 | ((uint32_t)b[1] << 8)
								 | ((uint32_t)b[2] << 16)
								 | ((uint32_t)b[3] << 24);
					}

					if (compute_file_crc32c(file_path, &crc))
					{
						if (crc != expected)
						{
							snprintf(msg, sizeof(msg),
									 "CRC32C mismatch: %s "
									 "(expected %08X, got %08X)",
									 entry_path, expected, crc);
							add_error(result, msg);
							errors++;
						}
					}
					else
					{
						snprintf(msg, sizeof(msg),
								 "Cannot read file for checksum: %s",
								 entry_path);
						add_warning(result, msg);
					}
				}
				/* NONE: presence already verified above */
			}
		}

		/* Reset for next entry */
		entry_path[0] = entry_algo[0] = entry_cksum[0] = '\0';
		entry_size = 0;
		have_path = have_algo = have_cksum = have_size = false;
	}

	fclose(fp);

	(void)checked;   /* available for debug output if needed */
	(void)errors;

done:
	if (result->error_count > 0)
		result->status = BACKUP_STATUS_ERROR;
	else if (result->warning_count > 0)
		result->status = BACKUP_STATUS_WARNING;

	return result;
}
