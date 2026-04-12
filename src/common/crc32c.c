/*
 * crc32c.c
 *
 * CRC32C (Castagnoli) software implementation.
 * Polynomial: 0x82F63B78 (bit-reversed 0x1EDC6F41).
 * INIT = ~0U, FIN = ~crc — matches PostgreSQL's pg_crc32c.
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

#include "crc32c.h"

static uint32_t crc32c_table[256];
static int      crc32c_table_ready = 0;

static void
init_crc32c_table(void)
{
	const uint32_t poly = 0x82F63B78U;
	int i, j;

	for (i = 0; i < 256; i++)
	{
		uint32_t crc = (uint32_t) i;

		for (j = 0; j < 8; j++)
			crc = (crc & 1) ? ((crc >> 1) ^ poly) : (crc >> 1);
		crc32c_table[i] = crc;
	}
	crc32c_table_ready = 1;
}

uint32_t
crc32c_update(uint32_t crc, const uint8_t *buf, size_t len)
{
	size_t i;

	if (!crc32c_table_ready)
		init_crc32c_table();

	for (i = 0; i < len; i++)
		crc = (crc >> 8) ^ crc32c_table[(crc ^ buf[i]) & 0xFFU];

	return crc;
}
