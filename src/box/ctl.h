#ifndef INCLUDES_TARANTOOL_CTL_H
#define INCLUDES_TARANTOOL_CTL_H

/*
 * Copyright 2010-2018, Tarantool AUTHORS, please see AUTHORS file.
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

#include <cfg.h>
#include <box/lua/ctl.h>

/** Global on-ctl_event triggers. */
extern struct rlist on_ctl_event;

enum ctl_event_type {
	CTL_EVENT_SYSTEM_SPACE_CREATE,
	CTL_EVENT_LOCAL_RECOVERY,
	CTL_EVENT_READ_ONLY,
	CTL_EVENT_READ_WRITE,
	CTL_EVENT_SHUTDOWN,
	CTL_EVENT_REPLICASET_ADD,
	CTL_EVENT_REPLICASET_REMOVE,
	CTL_EVENT_REPLICA_CONNECTION_ERROR,
	CTL_LAST_POS_GUARD,
};

struct on_ctl_event_ctx {
	enum ctl_event_type type;
	uint32_t replica_id;
};

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

/**
 * Runs on_ctl_event triggers with specified context.
 */
int
run_on_ctl_event_triggers(const struct on_ctl_event_ctx *result);

/**
 * Runs on_ctl_event trigger with specified type and
 * log error if any.
 */
void
on_ctl_event_type(enum ctl_event_type type);

int
cfg_reset_on_ctl_event();
#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* INCLUDES_TARANTOOL_LUA_CTL_H */
