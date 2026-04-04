/*
 * sha256.c
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

#include "sha256.h"
#include <string.h>
#include <stdio.h>

/* -----------------------------------------------------------------------
 * SHA-256 constants: first 32 bits of the fractional parts of the cube
 * roots of the first 64 primes (FIPS 180-4 § 4.2.2).
 * ----------------------------------------------------------------------- */
static const uint32_t K[64] = {
	0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
	0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
	0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
	0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
	0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
	0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
	0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
	0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
	0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
	0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
	0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
	0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
	0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
	0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
	0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
	0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
};

/* -----------------------------------------------------------------------
 * Bit-manipulation helpers (big-endian word I/O, rotations).
 * ----------------------------------------------------------------------- */

static inline uint32_t
be32(const uint8_t *p)
{
	return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
		   ((uint32_t)p[2] <<  8) |  (uint32_t)p[3];
}

static inline void
put_be32(uint8_t *p, uint32_t v)
{
	p[0] = (uint8_t)(v >> 24);
	p[1] = (uint8_t)(v >> 16);
	p[2] = (uint8_t)(v >>  8);
	p[3] = (uint8_t)(v);
}

static inline void
put_be64(uint8_t *p, uint64_t v)
{
	p[0] = (uint8_t)(v >> 56);
	p[1] = (uint8_t)(v >> 48);
	p[2] = (uint8_t)(v >> 40);
	p[3] = (uint8_t)(v >> 32);
	p[4] = (uint8_t)(v >> 24);
	p[5] = (uint8_t)(v >> 16);
	p[6] = (uint8_t)(v >>  8);
	p[7] = (uint8_t)(v);
}

#define ROTR32(x, n)  (((x) >> (n)) | ((x) << (32 - (n))))

#define CH(x, y, z)   (((x) & (y)) ^ (~(x) & (z)))
#define MAJ(x, y, z)  (((x) & (y)) ^ ((x) & (z)) ^ ((y) & (z)))
#define EP0(x)        (ROTR32(x,  2) ^ ROTR32(x, 13) ^ ROTR32(x, 22))
#define EP1(x)        (ROTR32(x,  6) ^ ROTR32(x, 11) ^ ROTR32(x, 25))
#define SIG0(x)       (ROTR32(x,  7) ^ ROTR32(x, 18) ^ ((x) >>  3))
#define SIG1(x)       (ROTR32(x, 17) ^ ROTR32(x, 19) ^ ((x) >> 10))

/* -----------------------------------------------------------------------
 * Process one 64-byte block.
 * ----------------------------------------------------------------------- */
static void
sha256_transform(SHA256Ctx *ctx, const uint8_t block[64])
{
	uint32_t w[64];
	uint32_t a, b, c, d, e, f, g, h;
	uint32_t t1, t2;
	int      i;

	/* Prepare message schedule */
	for (i = 0; i < 16; i++)
		w[i] = be32(block + i * 4);
	for (i = 16; i < 64; i++)
		w[i] = SIG1(w[i-2]) + w[i-7] + SIG0(w[i-15]) + w[i-16];

	/* Working variables */
	a = ctx->state[0];
	b = ctx->state[1];
	c = ctx->state[2];
	d = ctx->state[3];
	e = ctx->state[4];
	f = ctx->state[5];
	g = ctx->state[6];
	h = ctx->state[7];

	/* Compression */
	for (i = 0; i < 64; i++)
	{
		t1 = h + EP1(e) + CH(e, f, g) + K[i] + w[i];
		t2 = EP0(a) + MAJ(a, b, c);
		h = g;
		g = f;
		f = e;
		e = d + t1;
		d = c;
		c = b;
		b = a;
		a = t1 + t2;
	}

	ctx->state[0] += a;
	ctx->state[1] += b;
	ctx->state[2] += c;
	ctx->state[3] += d;
	ctx->state[4] += e;
	ctx->state[5] += f;
	ctx->state[6] += g;
	ctx->state[7] += h;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

void
sha256_init(SHA256Ctx *ctx)
{
	/* Initial hash values: first 32 bits of fractional parts of
	 * the square roots of the first 8 primes (FIPS 180-4 § 5.3.3). */
	ctx->state[0] = 0x6a09e667;
	ctx->state[1] = 0xbb67ae85;
	ctx->state[2] = 0x3c6ef372;
	ctx->state[3] = 0xa54ff53a;
	ctx->state[4] = 0x510e527f;
	ctx->state[5] = 0x9b05688c;
	ctx->state[6] = 0x1f83d9ab;
	ctx->state[7] = 0x5be0cd19;
	ctx->count   = 0;
	ctx->buf_len = 0;
}

void
sha256_update(SHA256Ctx *ctx, const void *data, size_t len)
{
	const uint8_t *p = (const uint8_t *)data;

	ctx->count += (uint64_t)len * 8;   /* track bit count */

	while (len > 0)
	{
		size_t room = 64 - ctx->buf_len;
		size_t copy = (len < room) ? len : room;

		memcpy(ctx->buf + ctx->buf_len, p, copy);
		ctx->buf_len += (uint32_t)copy;
		p   += copy;
		len -= copy;

		if (ctx->buf_len == 64)
		{
			sha256_transform(ctx, ctx->buf);
			ctx->buf_len = 0;
		}
	}
}

void
sha256_final(SHA256Ctx *ctx, uint8_t digest[SHA256_DIGEST_LENGTH])
{
	uint64_t bit_count = ctx->count;   /* save before padding touches nothing */
	uint32_t used      = ctx->buf_len;
	int      i;

	/*
	 * Append the 0x80 bit, then zero-pad to 56 bytes (mod 64), then
	 * the 8-byte big-endian message length.  All manipulation is direct
	 * on ctx->buf / ctx->state so we don't accidentally re-increment count.
	 */
	ctx->buf[used++] = 0x80;

	if (used > 56)
	{
		/* Not enough room for the length field — pad to end of block,
		 * process it, then start a fresh block. */
		memset(ctx->buf + used, 0, 64 - used);
		sha256_transform(ctx, ctx->buf);
		used = 0;
	}

	/* Zero-pad up to byte 56 */
	memset(ctx->buf + used, 0, 56 - used);

	/* Write the 64-bit bit count in big-endian at bytes 56..63 */
	put_be64(ctx->buf + 56, bit_count);
	sha256_transform(ctx, ctx->buf);

	/* Produce the digest */
	for (i = 0; i < 8; i++)
		put_be32(digest + i * 4, ctx->state[i]);
}

bool
sha256_file(const char *path, uint8_t digest[SHA256_DIGEST_LENGTH])
{
	FILE      *fp;
	SHA256Ctx  ctx;
	uint8_t    buf[65536];
	size_t     n;

	fp = fopen(path, "rb");
	if (fp == NULL)
		return false;

	sha256_init(&ctx);

	while ((n = fread(buf, 1, sizeof(buf), fp)) > 0)
		sha256_update(&ctx, buf, n);

	if (ferror(fp))
	{
		fclose(fp);
		return false;
	}

	fclose(fp);
	sha256_final(&ctx, digest);
	return true;
}

void
sha256_to_hex(const uint8_t digest[SHA256_DIGEST_LENGTH],
			  char hex[SHA256_HEX_LENGTH + 1])
{
	static const char hextab[] = "0123456789abcdef";
	int i;

	for (i = 0; i < SHA256_DIGEST_LENGTH; i++)
	{
		hex[i * 2]     = hextab[digest[i] >> 4];
		hex[i * 2 + 1] = hextab[digest[i] & 0x0f];
	}
	hex[SHA256_HEX_LENGTH] = '\0';
}
