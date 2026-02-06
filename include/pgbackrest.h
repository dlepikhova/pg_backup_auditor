/*
 * pgbackrest.h
 *
 * pgBackRest adapter interface
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

#ifndef PGBACKREST_H
#define PGBACKREST_H

#include "types.h"
#include "adapter.h"

/* Detect if path is a pgBackRest repository */
bool is_pgbackrest_repo(const char *path);

/* Scan pgBackRest repository for backups */
BackupInfo* scan_pgbackrest_backups(const char *repo_path);

/* Parse backup.info file */
BackupInfo* parse_pgbackrest_backup_info(const char *backup_info_path, const char *stanza_name);

/* Parse backup.manifest file for individual backup details */
bool parse_pgbackrest_manifest(BackupInfo *info, const char *manifest_path);

/* Get BackupAdapter for pgBackRest */
BackupAdapter* get_pgbackrest_adapter(void);

#endif /* PGBACKREST_H */
