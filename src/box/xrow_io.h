#ifndef TARANTOOL_XROW_IO_H_INCLUDED
#define TARANTOOL_XROW_IO_H_INCLUDED
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
#if defined(__cplusplus)
extern "C" {
#endif

#include "trivia/util.h"

struct ev_io;
struct ibuf;
struct xrow_header;

/** A batch of xrows. */
struct xrow_batch {
	/** Rows array. */
	struct xrow_header *rows;
	/** Count of rows, stored in @a rows array. */
	int count;
	/**
	 * Maximal count of rows, which can be stored in @a rows
	 * array.
	 */
	int capacity;
	/**
	 * Binary size of encoded!!! rows, stored in @a rows
	 * array.
	 */
	size_t bsize;
};

/** Create a batch. Start capacity is 0. */
void
xrow_batch_create(struct xrow_batch *batch);

/**
 * Reset count of rows to 0 to reuse allocated xrow_headers.
 * Do not free resources.
 */
void
xrow_batch_reset(struct xrow_batch *batch);

/** Set @a sync for all rows in batch. */
void
xrow_batch_set_sync(struct xrow_batch *batch, uint64_t sync);

/**
 * Create a new row or reuse an existing one from @a batch.
 * @param batch Batch to store row.
 * @retval Allocated row.
 */
struct xrow_header *
xrow_batch_new_row(struct xrow_batch *batch);

/** Free batch resources. */
void
xrow_batch_destroy(struct xrow_batch *batch);

/**
 * Read an iproto packet of rows as @a batch.
 * @param coio Input stream.
 * @param in Input buffer.
 * @param[out] batch Batch to store rows.
 */
void
coio_read_xrow_batch(struct ev_io *coio, struct ibuf *in,
		     struct xrow_batch *batch);

void
coio_read_xrow(struct ev_io *coio, struct ibuf *in, struct xrow_header *row);

void
coio_read_xrow_timeout_xc(struct ev_io *coio, struct ibuf *in,
			  struct xrow_header *row, double timeout);

void
coio_write_xrow(struct ev_io *coio, const struct xrow_header *row);

/**
 * Write @a batch of rows. If a batch is too big, it can be
 * sent in several parts, but still in a single iproto packet.
 * @param coio Output stream.
 * @param batch Batch to send.
 */
void
coio_write_xrow_batch(struct ev_io *coio, const struct xrow_batch *batch);

#if defined(__cplusplus)
} /* extern "C" */
#endif

#endif /* TARANTOOL_XROW_IO_H_INCLUDED */
