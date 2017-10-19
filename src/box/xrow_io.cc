/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "xrow_io.h"
#include "xrow.h"
#include "coio.h"
#include "coio_buf.h"
#include "error.h"
#include "msgpuck/msgpuck.h"
#include "errinj.h"
#include "iproto_constants.h"
#include "exception.h"

void
xrow_batch_create(struct xrow_batch *batch)
{
	memset(batch, 0, sizeof(*batch));
}

void
xrow_batch_reset(struct xrow_batch *batch)
{
	batch->count = 0;
	batch->bsize = 0;
}

void
xrow_batch_set_sync(struct xrow_batch *batch, uint64_t sync)
{
	uint32_t new_sync_size = mp_sizeof_uint(sync);
	uint32_t iproto_sync_size = mp_sizeof_uint(IPROTO_SYNC);
	for (int i = 0; i <  batch->count; ++i) {
		uint64_t row_sync = batch->rows[i].sync;
		if (row_sync == 0) {
			if (sync != 0) {
				batch->bsize += iproto_sync_size;
				batch->bsize += new_sync_size;
			}
		} else {
			if (sync == 0) {
				batch->bsize -= iproto_sync_size;
				batch->bsize -= mp_sizeof_uint(row_sync);
			} else {
				batch->bsize += new_sync_size;
				batch->bsize -= mp_sizeof_uint(row_sync);
			}
		}
		batch->rows[i].sync = sync;
	}
}

struct xrow_header *
xrow_batch_new_row(struct xrow_batch *batch)
{
	if (batch->count + 1 > batch->capacity) {
		size_t new_capacity = (batch->capacity + 1) * 2;
		size_t size = sizeof(batch->rows[0]) * new_capacity;
		struct xrow_header *rows =
			(struct xrow_header *)realloc(batch->rows, size);
		if (rows == NULL)
			tnt_raise(OutOfMemory, size, "realloc", "rows");
		batch->rows = rows;
		batch->capacity = new_capacity;
	}
	return &batch->rows[batch->count++];
}

void
xrow_batch_destroy(struct xrow_batch *batch)
{
	free(batch->rows);
}

void
coio_read_xrow_batch(struct ev_io *coio, struct ibuf *in,
		     struct xrow_batch *batch)
{
	/* Read fixed header */
	if (ibuf_used(in) < 1)
		coio_breadn(coio, in, 1);

	/* Read length */
	if (mp_typeof(*in->rpos) != MP_UINT) {
		tnt_raise(ClientError, ER_INVALID_MSGPACK,
			  "packet length");
	}
	ssize_t to_read = mp_check_uint(in->rpos, in->wpos);
	if (to_read > 0)
		coio_breadn(coio, in, to_read);

	uint32_t bsize = mp_decode_uint((const char **) &in->rpos);
	/* Read header and body */
	to_read = bsize - ibuf_used(in);
	if (to_read > 0)
		coio_breadn(coio, in, to_read);
	assert(bsize > 0);
	char *rpos = in->rpos;
	const char *end = rpos + bsize;
	do {
		struct xrow_header *row = xrow_batch_new_row(batch);
		const char *row_begin = rpos;
		xrow_header_decode_xc(row, (const char **) &rpos, end);
		batch->bsize += rpos - row_begin;
	} while (batch->bsize != bsize);
	in->rpos = rpos;
}

void
coio_read_xrow(struct ev_io *coio, struct ibuf *in, struct xrow_header *row)
{
	/* Read fixed header */
	if (ibuf_used(in) < 1)
		coio_breadn(coio, in, 1);

	/* Read length */
	if (mp_typeof(*in->rpos) != MP_UINT) {
		tnt_raise(ClientError, ER_INVALID_MSGPACK,
			  "packet length");
	}
	ssize_t to_read = mp_check_uint(in->rpos, in->wpos);
	if (to_read > 0)
		coio_breadn(coio, in, to_read);

	uint32_t len = mp_decode_uint((const char **) &in->rpos);

	/* Read header and body */
	to_read = len - ibuf_used(in);
	if (to_read > 0)
		coio_breadn(coio, in, to_read);

	xrow_header_decode_xc(row, (const char **) &in->rpos, in->rpos + len);
}

void
coio_read_xrow_timeout_xc(struct ev_io *coio, struct ibuf *in,
			  struct xrow_header *row, ev_tstamp timeout)
{
	ev_tstamp start, delay;
	coio_timeout_init(&start, &delay, timeout);
	/* Read fixed header */
	if (ibuf_used(in) < 1)
		coio_breadn_timeout(coio, in, 1, delay);
	coio_timeout_update(start, &delay);

	/* Read length */
	if (mp_typeof(*in->rpos) != MP_UINT) {
		tnt_raise(ClientError, ER_INVALID_MSGPACK,
			  "packet length");
	}
	ssize_t to_read = mp_check_uint(in->rpos, in->wpos);
	if (to_read > 0)
		coio_breadn_timeout(coio, in, to_read, delay);
	coio_timeout_update(start, &delay);

	uint32_t len = mp_decode_uint((const char **) &in->rpos);

	/* Read header and body */
	to_read = len - ibuf_used(in);
	if (to_read > 0)
		coio_breadn_timeout(coio, in, to_read, delay);

	xrow_header_decode_xc(row, (const char **) &in->rpos, in->rpos + len);
}

void
coio_write_xrow(struct ev_io *coio, const struct xrow_header *row)
{
	struct iovec iov[XROW_IOVMAX];
	int iovcnt = xrow_to_iovec_xc(row, iov);
	ERROR_INJECT(ERRINJ_COIO_PARTIAL_WRITE_ROW, {
		iovcnt = 1;
		iov[0].iov_len /= 2;
		coio_writev(coio, iov, iovcnt, 0);
		tnt_raise(SocketError, coio->fd, "errinj partial write");
	});
	coio_writev(coio, iov, iovcnt, 0);
}

void
coio_write_xrow_batch(struct ev_io *coio, const struct xrow_batch *batch)
{
	struct iovec iov[XROW_BATCH_SIZE];
	char fixheader[XROW_HEADER_LEN_MAX];
	/* In a first iov send an iproto packet header. */
	iov[0].iov_base = fixheader;
	fixheader[0] = 0xce; /* MP_UINT32 */
	store_u32(fixheader + 1, mp_bswap_u32(batch->bsize));
	iov[0].iov_len = 5;
	int iov_count = 1, i = 0;
	size_t written = 0;

	/* Then send rows sequentially. */
	for (; i < batch->count; ++i) {
		/*
		 * If the batch is too big, send the batch in
		 * parts.
		 */
		if (iov_count + XROW_IOVMAX >= XROW_BATCH_SIZE) {
			written += coio_writev(coio, iov, iov_count, 0);
			iov_count = 0;
		}
		iov_count += xrow_header_encode(&batch->rows[i],
						batch->rows[i].sync,
						&iov[iov_count], 0);
	}
	if (iov_count > 0) {
		ERROR_INJECT(ERRINJ_COIO_PARTIAL_WRITE_ROW, {
			iov[0].iov_len /= 2;
			coio_writev(coio, iov, 1, 0);
			tnt_raise(SocketError, coio->fd, "errinj partial write");
		});
		written += coio_writev(coio, iov, iov_count, 0);
	}
	assert(written == batch->bsize + 5);
}
