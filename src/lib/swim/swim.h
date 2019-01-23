#ifndef TARANTOOL_SWIM_H_INCLUDED
#define TARANTOOL_SWIM_H_INCLUDED
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
#if defined(__cplusplus)
extern "C" {
#endif

struct info_handler;
struct swim;
struct swim_transport_vtab;

/**
 * Create new SWIM instance. Just creation without binding,
 * setting any parameters or something. Allocation and
 * initialization only.
 */
struct swim *
swim_new(const struct swim_transport_vtab *transport_vtab);

/**
 * Configure or reconfigure a SWIM instance.
 *
 * @param swim SWIM instance to configure.
 * @param uri URI in the format "ip:port".
 * @param heartbeat_rate Rate of sending round messages. It does
 *        mean that each member will be checked each
 *        @heartbeat_rate seconds. It is rather the protocol
 *        speed. Protocol period depends on member count and
 *        @heartbeat_rate.
 * @param ack_timeout Time in seconds after which a ping is
 *        considered to be unacknowledged.
 *
 * @retval 0 Success.
 * @retval -1 Error.
 */
int
swim_cfg(struct swim *swim, const char *uri, double heartbeat_rate,
	 double ack_timeout);

/**
 * Stop listening and broadcasting messages, cleanup all internal
 * structures, free memory.
 */
void
swim_delete(struct swim *swim);

/**
 * Add a new member. It is added to the members table and pinned.
 * SWIM will ping the member, but never will delete him, even if
 * pings fail.
 */
int
swim_add_member(struct swim *swim, const char *uri);

/** Silently remove a member from members table. */
int
swim_remove_member(struct swim *swim, const char *uri);

/** Dump member statuses into @a info. */
void
swim_info(struct swim *swim, struct info_handler *info);

#if defined(__cplusplus)
}
#endif

#endif /* TARANTOOL_SWIM_H_INCLUDED */
