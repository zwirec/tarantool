#ifndef TARANTOOL_SWIM_IO_H_INCLUDED
#define TARANTOOL_SWIM_IO_H_INCLUDED
/*
 * Copyright 2010-2019, Tarantool AUTHORS, please see AUTHORS file.
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
#include "trivia/util.h"
#include "small/rlist.h"
#include "salad/stailq.h"
#include "swim_transport.h"
#include "evio.h"
#include <stdbool.h>
#include <arpa/inet.h>

#if defined(__cplusplus)
extern "C" {
#endif

struct swim_task;
struct swim_scheduler;

enum {
	/**
	 * Default MTU is 1500. MTU (when IPv4 is used) consists
	 * of IPv4 header, UDP header, Data. IPv4 has 20 bytes
	 * header, UDP - 8 bytes. So Data = 1500 - 20 - 8 = 1472.
	 * TODO: adapt to other MTUs which can be reduced in some
	 * networks by their admins.
	 */
	UDP_PACKET_SIZE = 1472,
};

/**
 * A UDP packet. Works as an allocator, allowing to fill its body
 * gradually.
 */
struct swim_packet {
	/** Last valid position in the body. */
	char *pos;
	/** Position beyond pos, contains unfinished data. */
	char *next_pos;
	/**
	 * Starting position of body in the buffer. Can be
	 * different from buf during decoding, when it is moved
	 * after decoding meta section.
	 */
	char *body;
	/** Alias for swim_packet.buf. Just sugar. */
	char meta[0];
	/** Packet body buffer. */
	char buf[UDP_PACKET_SIZE];
	/**
	 * Pointer to the end of the body. Just syntax sugar to do
	 * not write 'body + sizeof(body)' each time.
	 */
	char end[0];
};

static inline char *
swim_packet_reserve(struct swim_packet *packet, int size)
{
	return packet->next_pos + size > packet->end ? NULL : packet->next_pos;
}

static inline void
swim_packet_advance(struct swim_packet *packet, int size)
{
	assert(packet->next_pos + size <= packet->end);
	packet->next_pos += size;
}

static inline char *
swim_packet_alloc(struct swim_packet *packet, int size)
{
	char *res = swim_packet_reserve(packet, size);
	if (res == NULL)
		return NULL;
	swim_packet_advance(packet, size);
	return res;
}

static inline void
swim_packet_flush(struct swim_packet *packet)
{
	assert(packet->next_pos >= packet->pos);
	packet->pos = packet->next_pos;
}

static inline void
swim_packet_create(struct swim_packet *packet)
{
	packet->pos = packet->body;
	packet->next_pos = packet->body;
	packet->body = packet->buf;
}

typedef void (*swim_scheduler_on_input_f)(struct swim_scheduler *scheduler,
					  const struct swim_packet *packet,
					  const struct sockaddr_in *src);

struct swim_scheduler {
	/** Transport used to receive packets. */
	struct swim_transport transport;
	/** Function called when a packet is received. */
	swim_scheduler_on_input_f on_input;
	/**
	 * Event dispatcher of incomming messages. Takes them from
	 * network.
	 */
	struct ev_io input;
	/**
	 * Event dispatcher of outcomming messages. Takes tasks
	 * from queue_output.
	 */
	struct ev_io output;
	/** Queue of output tasks ready to write now. */
	struct rlist queue_output;
};

void
swim_scheduler_create(struct swim_scheduler *scheduler,
		      swim_scheduler_on_input_f on_input,
		      const struct swim_transport_vtab *transport_vtab);

int
swim_scheduler_bind(struct swim_scheduler *scheduler, struct sockaddr_in *addr);

void
swim_scheduler_destroy(struct swim_scheduler *scheduler);

/**
 * Each SWIM component in a common case independently may want to
 * push some data into the network. Dissemination sends events,
 * failure detection sends pings, acks. Anti-entropy sends member
 * tables. The intention to send a data is called IO task and is
 * stored in a queue that is dispatched when output is possible.
 */
typedef void (*swim_task_f)(struct swim_task *, int rc);

struct swim_task {
	/** Function called when the task has completed. */
	swim_task_f complete;
	/** Context data. For complete() callback, for example. */
	void *ctx;
	/** Packet to send. */
	struct swim_packet packet;
	/** Destination address. */
	struct sockaddr_in dst;
	/** Place in a queue of tasks. */
	struct rlist in_queue_output;
};

void
swim_task_schedule(struct swim_task *task, const struct sockaddr_in *dst,
		   struct swim_scheduler *scheduler);

void
swim_task_create(struct swim_task *task, swim_task_f complete, void *ctx);

static inline bool
swim_task_is_active(struct swim_task *task)
{
	return ! rlist_empty(&task->in_queue_output);
}

static inline void
swim_task_destroy(struct swim_task *task)
{
	rlist_del_entry(task, in_queue_output);
}

#if defined(__cplusplus)
}
#endif

#endif /* TARANTOOL_SWIM_IO_H_INCLUDED */