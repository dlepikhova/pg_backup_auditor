/*
 * sha256.h
 *
 * Minimal self-contained SHA-256 implementation (FIPS 180-4).
 * No external dependencies.
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

#ifndef SHA256_H
#define SHA256_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#define SHA256_DIGEST_LENGTH  32
#define SHA256_HEX_LENGTH     64   /* 32 bytes × 2 hex digits, no NUL */

typedef struct {
	uint32_t state[8];
	uint64_t count;         /* total bits processed */
	uint8_t  buf[64];       /* partial block buffer */
	uint32_t buf_len;       /* bytes used in buf */
} SHA256Ctx;

void sha256_init(SHA256Ctx *ctx);
void sha256_update(SHA256Ctx *ctx, const void *data, size_t len);
void sha256_final(SHA256Ctx *ctx, uint8_t digest[SHA256_DIGEST_LENGTH]);

/*
 * One-shot helpers.
 *
 * sha256_file   — hash an entire file; returns false on I/O error.
 * sha256_to_hex — convert a raw 32-byte digest to a 65-byte NUL-terminated
 *                 lowercase hex string.
 */
bool sha256_file(const char *path, uint8_t digest[SHA256_DIGEST_LENGTH]);
void sha256_to_hex(const uint8_t digest[SHA256_DIGEST_LENGTH],
				   char hex[SHA256_HEX_LENGTH + 1]);

#endif /* SHA256_H */
