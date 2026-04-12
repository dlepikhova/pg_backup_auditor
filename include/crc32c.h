/*
 * crc32c.h
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

#ifndef CRC32C_H
#define CRC32C_H

#include <stddef.h>
#include <stdint.h>

/*
 * Update a running CRC32C accumulator over 'len' bytes.
 * Initializes the lookup table on first call.
 * Usage: start with crc = ~0U; finalize with ~crc.
 */
uint32_t crc32c_update(uint32_t crc, const uint8_t *buf, size_t len);

#endif /* CRC32C_H */
