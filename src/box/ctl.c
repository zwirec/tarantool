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
#include <lib/small/small/rlist.h>
#include <trigger.h>
#include <box/ctl.h>
#include "errcode.h"
#include "error.h"
#include <exception.h>

RLIST_HEAD(on_ctl_event);

const char* type2str[CTL_LAST_POS_GUARD] = {
	"system space recovery", // CTL_EVENT_SYSTEM_SPACE_RECOVERY
	"local recovery",	 // CTL_EVENT_LOCAL_RECOVERY
	"read only",		 // CTL_EVENT_READ_ONLY
	"read write",		 // CTL_EVENT_READ_WRITE
	"shutdown",		 // CTL_EVENT_SHUTDOWN
	"replicaset add",	 // CTL_EVENT_REPLICASET_ADD
	"replicaset remove",	 // CTL_EVENT_REPLICASET_REMOVE
	"replica connect error", // CTL_EVENT_REPLICA_CONNECTION_ERROR
};

int
run_on_ctl_event_triggers(const struct on_ctl_event_ctx *result) {
	return trigger_run(&on_ctl_event, (void *) result);
}

void
on_ctl_event_type(enum ctl_event_type type)
{
	// TODO: check if local cached variable on_ctl_event is set.
	// if(!on_ctl_event_cached)
	//	return;
	struct on_ctl_event_ctx ctx = {};
	ctx.type = type;
	if (run_on_ctl_event_triggers(&ctx) < 0)
		say_error("ctl_trigger error in %s: %s", type2str[type],
                          diag_last_error(diag_get())->errmsg);
}

int
cfg_reset_on_ctl_event()
{
	if (cfg_reset_trigger("on_ctl_event", &on_ctl_event,
		lbox_push_on_ctl_event, NULL) < 0) {
		diag_set(ClientError, ER_CFG, "on_ctl_event",
			 "expected function or table");
		return -1;
	}
	return 0;
}
