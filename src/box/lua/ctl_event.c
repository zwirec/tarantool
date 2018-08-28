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

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "trigger.h"
#include "box/ctl_event.h"
#include "box/applier.h"

#include "lua/trigger.h"
#include "lua/utils.h"

static int
lbox_ctl_event_const(struct lua_State *L)
{
	lua_newtable(L);
	lua_pushstring(L, "RECOVERY");
	lua_pushinteger(L, CTL_RECOVERY);
	lua_settable(L, -3);
	lua_pushstring(L, "SPACE");
	lua_pushinteger(L, CTL_SPACE);
	lua_settable(L, -3);
	lua_pushstring(L, "SHUTDOWN");
	lua_pushinteger(L, CTL_SHUTDOWN);
	lua_settable(L, -3);

	lua_pushstring(L, "RECOVERY_SNAPSHOT_START");
	lua_pushinteger(L, CTL_RECOVERY_SNAPSHOT_START);
	lua_settable(L, -3);
	lua_pushstring(L, "RECOVERY_SNAPSHOT_DONE");
	lua_pushinteger(L, CTL_RECOVERY_SNAPSHOT_DONE);
	lua_settable(L, -3);
	lua_pushstring(L, "RECOVERY_HOT_STANDBY_START");
	lua_pushinteger(L, CTL_RECOVERY_HOT_STANDBY_START);
	lua_settable(L, -3);
	lua_pushstring(L, "RECOVERY_HOT_STANDBY_DONE");
	lua_pushinteger(L, CTL_RECOVERY_HOT_STANDBY_DONE);
	lua_settable(L, -3);
	lua_pushstring(L, "RECOVERY_XLOGS_DONE");
	lua_pushinteger(L, CTL_RECOVERY_XLOGS_DONE);
	lua_settable(L, -3);
	lua_pushstring(L, "RECOVERY_BOOTSTRAP_START");
	lua_pushinteger(L, CTL_RECOVERY_BOOTSTRAP_START);
	lua_settable(L, -3);
	lua_pushstring(L, "RECOVERY_BOOTSTRAP_DONE");
	lua_pushinteger(L, CTL_RECOVERY_BOOTSTRAP_DONE);
	lua_settable(L, -3);
	lua_pushstring(L, "RECOVERY_INITIAL_JOIN_START");
	lua_pushinteger(L, CTL_RECOVERY_INITIAL_JOIN_START);
	lua_settable(L, -3);
	lua_pushstring(L, "RECOVERY_INITIAL_JOIN_DONE");
	lua_pushinteger(L, CTL_RECOVERY_INITIAL_JOIN_DONE);
	lua_settable(L, -3);
	lua_pushstring(L, "RECOVERY_FINAL_JOIN_DONE");
	lua_pushinteger(L, CTL_RECOVERY_FINAL_JOIN_DONE);
	lua_settable(L, -3);

	lua_pushstring(L, "SPACE_CREATE");
	lua_pushinteger(L, CTL_SPACE_CREATE);
	lua_settable(L, -3);
	lua_pushstring(L, "SPACE_ALTER");
	lua_pushinteger(L, CTL_SPACE_ALTER);
	lua_settable(L, -3);
	lua_pushstring(L, "SPACE_DELETE");
	lua_pushinteger(L, CTL_SPACE_DELETE);
	lua_settable(L, -3);

	lua_pushstring(L, "APPLIER_OFF");
	lua_pushinteger(L, APPLIER_OFF);
	lua_settable(L, -3);
	lua_pushstring(L, "APPLIER_CONNECT");
	lua_pushinteger(L, APPLIER_CONNECT);
	lua_settable(L, -3);
	lua_pushstring(L, "APPLIER_CONNECTED");
	lua_pushinteger(L, APPLIER_CONNECTED);
	lua_settable(L, -3);
	lua_pushstring(L, "APPLIER_AUTH");
	lua_pushinteger(L, APPLIER_AUTH);
	lua_settable(L, -3);
	lua_pushstring(L, "APPLIER_READY");
	lua_pushinteger(L, APPLIER_READY);
	lua_settable(L, -3);
	lua_pushstring(L, "APPLIER_INITIAL_JOIN");
	lua_pushinteger(L, APPLIER_INITIAL_JOIN);
	lua_settable(L, -3);
	lua_pushstring(L, "APPLIER_FINAL_JOIN");
	lua_pushinteger(L, APPLIER_FINAL_JOIN);
	lua_settable(L, -3);
	lua_pushstring(L, "APPLIER_JOINED");
	lua_pushinteger(L, APPLIER_JOINED);
	lua_settable(L, -3);
	lua_pushstring(L, "APPLIER_SYNC");
	lua_pushinteger(L, APPLIER_SYNC);
	lua_settable(L, -3);
	lua_pushstring(L, "APPLIER_FOLLOW");
	lua_pushinteger(L, APPLIER_FOLLOW);
	lua_settable(L, -3);
	lua_pushstring(L, "APPLIER_STOPPED");
	lua_pushinteger(L, APPLIER_STOPPED);
	lua_settable(L, -3);
	lua_pushstring(L, "APPLIER_DISCONNECTED");
	lua_pushinteger(L, APPLIER_DISCONNECTED);
	lua_settable(L, -3);
	lua_pushstring(L, "APPLIER_LOADING");
	lua_pushinteger(L, APPLIER_LOADING);
	lua_settable(L, -3);

	return 1;
}

static int
lbox_push_on_ctl_event(struct lua_State *L, void *data)
{
	struct on_ctl_event *event = (struct on_ctl_event *)data;
	switch (event->type) {
	case CTL_RECOVERY:
		lua_newtable(L);
		lua_pushstring(L, "type");
		lua_pushinteger(L, CTL_RECOVERY);
		lua_settable(L, -3);
		lua_pushstring(L, "status");
		lua_pushinteger(L, event->recovery.status);
		lua_settable(L, -3);
		break;
	case CTL_SPACE:
		lua_newtable(L);
		lua_pushstring(L, "type");
		lua_pushinteger(L, CTL_SPACE);
		lua_settable(L, -3);
		lua_pushstring(L, "action");
		lua_pushinteger(L, event->space.action);
		lua_settable(L, -3);
		lua_pushstring(L, "space_id");
		lua_pushinteger(L, event->space.space_id);
		lua_settable(L, -3);
		break;
	case CTL_SHUTDOWN:
		lua_newtable(L);
		lua_pushstring(L, "type");
		lua_pushinteger(L, CTL_SHUTDOWN);
		lua_settable(L, -3);
		break;
	case CTL_APPLIER:
		lua_newtable(L);
		lua_pushstring(L, "type");
		lua_pushinteger(L, CTL_APPLIER);
		lua_settable(L, -3);
		lua_pushstring(L, "replica");
		lua_pushstring(L, tt_uuid_str(&event->applier.replica_uuid));
		lua_settable(L, -3);
		lua_pushstring(L, "status");
		lua_pushinteger(L, event->applier.status);
		lua_settable(L, -3);
		break;
	default:
		lua_pushnil(L);
	}
	return 1;
}

static int
lbox_on_ctl_event(struct lua_State *L)
{
	return lbox_trigger_reset(L, 2, &on_ctl_trigger,
				  lbox_push_on_ctl_event, NULL);
}

static const struct luaL_Reg lbox_ctl_lib[] = {
	{"const", lbox_ctl_event_const},
	{"on_ctl_event", lbox_on_ctl_event},
	{NULL, NULL}
};

void
box_lua_ctl_event_init(struct lua_State *L)
{
	luaL_register_module(L, "box.ctl_event", lbox_ctl_lib);
	lua_pop(L, 1);
}
