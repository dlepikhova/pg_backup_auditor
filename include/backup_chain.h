/*
 * backup_chain.h
 *
 * Backup chain grouping and analysis utilities
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

#ifndef BACKUP_CHAIN_H
#define BACKUP_CHAIN_H

#include "pg_backup_auditor.h"

typedef struct {
	BackupInfo  *root;      /* Root FULL backup; NULL = orphaned group */
	BackupInfo **members;   /* Sorted by start_time, oldest first */
	int          count;
	int          capacity;
} BackupChain;

BackupInfo *backup_chain_find_backup(BackupInfo *list, const char *id);
BackupInfo *backup_chain_find_root(BackupInfo *backup, BackupInfo *all_backups);
BackupChain *backup_chain_build(BackupInfo *all_backups, int *nchains);
void backup_chain_free(BackupChain *chains, int count);

#endif
