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
#include "box/lua/ctl.h"

#include <tarantool_ev.h>

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
#include <lua/trigger.h>
#include <box/ctl.h>

#include "lua/utils.h"

#include "box/box.h"

static int
lbox_ctl_wait_ro(struct lua_State *L)
{
	int index = lua_gettop(L);
	double timeout = TIMEOUT_INFINITY;
	if (index > 0)
		timeout = luaL_checknumber(L, 1);
	if (box_wait_ro(true, timeout) != 0)
		return luaT_error(L);
	return 0;
}

static int
lbox_ctl_wait_rw(struct lua_State *L)
{
	int index = lua_gettop(L);
	double timeout = TIMEOUT_INFINITY;
	if (index > 0)
		timeout = luaL_checknumber(L, 1);
	if (box_wait_ro(false, timeout) != 0)
		return luaT_error(L);
	return 0;
}


int
lbox_push_on_ctl_event(struct lua_State *L, void *event)
{
	struct on_ctl_event_ctx *ctx = (struct on_ctl_event_ctx *) event;
	lua_newtable(L);
	lua_pushstring(L, "type");
	lua_pushinteger(L, ctx->type);
	lua_settable(L, -3);

	if (ctx->type == CTL_EVENT_REPLICASET_ADD ||
		ctx->type == CTL_EVENT_REPLICASET_REMOVE) {
		lua_pushstring(L, "replica_id");
		luaL_pushuint64(L, ctx->replica_id);
		lua_settable(L, -3);
	}
	return 1;
}

static int
lbox_on_ctl_event(struct lua_State *L)
{
	return lbox_trigger_reset(L, 2, &on_ctl_event,
				  lbox_push_on_ctl_event, NULL);
}


static const struct luaL_Reg lbox_ctl_lib[] = {
	{"wait_ro", lbox_ctl_wait_ro},
	{"wait_rw", lbox_ctl_wait_rw},
	{"on_ctl_event", lbox_on_ctl_event},
	{NULL, NULL}
};

void
box_lua_ctl_init(struct lua_State *L)
{
	luaL_register_module(L, "box.ctl", lbox_ctl_lib);
	lua_pop(L, 1);

	luaL_findtable(L, LUA_GLOBALSINDEX, "box.ctl", 1);
	lua_newtable(L);
	lua_setfield(L, -2, "event");
	lua_getfield(L, -1, "event");

	lua_pushnumber(L, CTL_EVENT_SYSTEM_SPACE_RECOVERY);
	lua_setfield(L, -2, "SYSTEM_SPACE_RECOVERY");
	lua_pushnumber(L, CTL_EVENT_LOCAL_RECOVERY);
	lua_setfield(L, -2, "LOCAL_RECOVERY");
	lua_pushnumber(L, CTL_EVENT_READ_ONLY);
	lua_setfield(L, -2, "READ_ONLY");
	lua_pushnumber(L, CTL_EVENT_READ_WRITE);
	lua_setfield(L, -2, "READ_WRITE");
	lua_pushnumber(L, CTL_EVENT_SHUTDOWN);
	lua_setfield(L, -2, "SHUTDOWN");
	lua_pushnumber(L, CTL_EVENT_REPLICASET_ADD);
	lua_setfield(L, -2, "REPLICASET_ADD");
	lua_pushnumber(L, CTL_EVENT_REPLICASET_REMOVE);
	lua_setfield(L, -2, "REPLICASET_REMOVE");
	lua_pop(L, 2); /* box, ctl */
}
