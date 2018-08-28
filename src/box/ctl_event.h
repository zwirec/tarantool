#ifndef BOX_CTL_EVENT_H
#define BOX_CTL_EVENT_H

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

#include <stdint.h>

#include "small/rlist.h"
#include "tt_uuid.h"

/* Ctl event types. */
#define CTL_RECOVERY     1
#define CTL_SPACE        2
#define CTL_SHUTDOWN     3
#define CTL_APPLIER      4

/* CTL_RECOVERY event status. */
#define CTL_RECOVERY_SNAPSHOT_START       1
#define CTL_RECOVERY_SNAPSHOT_DONE        2
#define CTL_RECOVERY_HOT_STANDBY_START    3
#define CTL_RECOVERY_HOT_STANDBY_DONE     4
#define CTL_RECOVERY_XLOGS_DONE           5
#define CTL_RECOVERY_BOOTSTRAP_START      6
#define CTL_RECOVERY_BOOTSTRAP_DONE       7
#define CTL_RECOVERY_INITIAL_JOIN_START   8
#define CTL_RECOVERY_INITIAL_JOIN_DONE    9
#define CTL_RECOVERY_FINAL_JOIN_DONE      10

#define CTL_SPACE_CREATE    1
#define CTL_SPACE_ALTER     2
#define CTL_SPACE_DELETE    3

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

extern struct rlist on_ctl_trigger;


/* CTL_RECOVERY event specific data. */
struct on_ctl_recovery_event {
	uint32_t status;
};

/* CTL_SPACE event specific data. */
struct on_ctl_space_event {
	uint32_t action;
	uint32_t space_id;
};

struct on_ctl_applier_event {
	struct tt_uuid replica_uuid;
	uint32_t status;
};

struct on_ctl_event {
	uint32_t type;
	union {
		struct on_ctl_recovery_event recovery;
		struct on_ctl_space_event space;
		struct on_ctl_applier_event applier;
	};
};


#if defined(__cplusplus)
}
#endif /* defined(__cplusplus) */


#endif
