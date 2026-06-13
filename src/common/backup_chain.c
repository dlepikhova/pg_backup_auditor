/*
 * backup_chain.c
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

#include "backup_chain.h"
#include <stdlib.h>
#include <string.h>

BackupInfo *
backup_chain_find_backup(BackupInfo *list, const char *id)
{
	for (BackupInfo *b = list; b != NULL; b = b->next)
		if (strcmp(b->backup_id, id) == 0)
			return b;
	return NULL;
}

BackupInfo *
backup_chain_find_root(BackupInfo *backup, BackupInfo *all_backups)
{
	BackupInfo *cur = backup;
	for (int depth = 0; depth < 1000; depth++)
	{
		if (cur->type == BACKUP_TYPE_FULL)
			return cur;
		if (cur->parent_backup_id[0] == '\0')
			return NULL;
		cur = backup_chain_find_backup(all_backups, cur->parent_backup_id);
		if (cur == NULL)
			return NULL;
	}
	return NULL;
}

static int
backup_chain_compare_by_time(const void *a, const void *b)
{
	time_t ta = (*(const BackupInfo **)a)->start_time;
	time_t tb = (*(const BackupInfo **)b)->start_time;
	return (ta > tb) - (ta < tb);
}

static bool
backup_chain_append(BackupChain *chain, BackupInfo *backup)
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

BackupChain *
backup_chain_build(BackupInfo *all_backups, int *nchains)
{
	int full_count = 0;
	for (BackupInfo *b = all_backups; b != NULL; b = b->next)
		if (b->type == BACKUP_TYPE_FULL)
			full_count++;

	BackupChain *chains = calloc(full_count + 1, sizeof(BackupChain));
	if (chains == NULL)
		return NULL;

	int ci = 0;

	/* One chain per FULL */
	for (BackupInfo *b = all_backups; b != NULL; b = b->next)
	{
		if (b->type != BACKUP_TYPE_FULL)
			continue;
		chains[ci].root = b;
		backup_chain_append(&chains[ci], b);
		ci++;
	}

	/* Assign non-FULL backups to their chain or the orphaned bucket */
	int orphan = full_count;
	for (BackupInfo *b = all_backups; b != NULL; b = b->next)
	{
		if (b->type == BACKUP_TYPE_FULL)
			continue;

		BackupInfo *root   = backup_chain_find_root(b, all_backups);
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
		backup_chain_append(&chains[target], b);
	}

	/* Include orphaned bucket only if non-empty */
	int total = full_count;
	if (chains[orphan].count > 0)
		total++;

	/* Sort members within each chain by start_time */
	for (int i = 0; i < total; i++)
	{
		if (chains[i].count > 1)
			qsort(chains[i].members, chains[i].count,
				  sizeof(BackupInfo *), backup_chain_compare_by_time);
	}

	*nchains = total;
	return chains;
}

void
backup_chain_free(BackupChain *chains, int count)
{
	if (chains == NULL)
		return;
	for (int i = 0; i < count; i++)
		free(chains[i].members);
	free(chains);
}
