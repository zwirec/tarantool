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
#include "box/lua/init.h"

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include "lua/utils.h" /* luaT_error() */
#include "lua/trigger.h"

#include "box/box.h"
#include "box/txn.h"
#include "box/vclock.h"

#include "box/lua/error.h"
#include "box/lua/tuple.h"
#include "box/lua/call.h"
#include "box/lua/slab.h"
#include "box/lua/index.h"
#include "box/lua/space.h"
#include "box/lua/sequence.h"
#include "box/lua/misc.h"
#include "box/lua/stat.h"
#include "box/lua/info.h"
#include "box/lua/ctl.h"
#include "box/lua/session.h"
#include "box/lua/net_box.h"
#include "box/lua/cfg.h"
#include "box/lua/xlog.h"
#include "box/lua/console.h"
#include "box/lua/tuple.h"
#include "box/lua/sql.h"
#include "box/lua/merger.h"

extern char session_lua[],
	tuple_lua[],
	schema_lua[],
	load_cfg_lua[],
	xlog_lua[],
	feedback_daemon_lua[],
	net_box_lua[],
	upgrade_lua[],
	console_lua[];

static const char *lua_sources[] = {
	"box/session", session_lua,
	"box/tuple", tuple_lua,
	"box/schema", schema_lua,
	"box/feedback_daemon", feedback_daemon_lua,
	"box/upgrade", upgrade_lua,
	"box/net_box", net_box_lua,
	"box/console", console_lua,
	"box/load_cfg", load_cfg_lua,
	"box/xlog", xlog_lua,
	NULL
};

static int
lbox_commit(lua_State *L)
{
	if (box_txn_commit() != 0)
		return luaT_error(L);
	return 0;
}

static int
lbox_rollback(lua_State *L)
{
	(void)L;
	if (box_txn_rollback() != 0)
		return luaT_error(L);
	return 0;
}

/**
 * Get a next txn statement from the current transaction. This is
 * a C closure and 2 upvalues should be available: first is a
 * transaction id, second is a previous statement. This function
 * works only inside on commit trigger of the concrete
 * transaction.
 * It takes two parameters according to Lua 'for' semantics: the
 * first is iterator (that here is nil and unused), the second is
 * key of iteration - here it is integer growing from 1 to
 * txn->n_rows.
 * It returns values with respect to Lua 'for' as well: the first
 * is the next key (previous + 1), the 2th - 4th are a statement
 * attributes: old tuple or nil, new tuple or nil, space id.
 */
static int
lbox_txn_iterator_next(struct lua_State *L)
{
	struct txn *txn = in_txn();
	int64_t txn_id = luaL_toint64(L, lua_upvalueindex(1));
	if (txn == NULL || txn->id != txn_id) {
		diag_set(ClientError, ER_CURSOR_NO_TRANSACTION);
		return luaT_error(L);
	}
	struct txn_stmt *stmt =
		(struct txn_stmt *) lua_topointer(L, lua_upvalueindex(2));
	if (stmt == NULL)
		return 0;
	while (stmt->row == NULL) {
		stmt = stailq_next_entry(stmt, next);
		if (stmt == NULL) {
			lua_pushnil(L);
			lua_replace(L, lua_upvalueindex(2));
			return 0;
		}
	}
	lua_pushinteger(L, lua_tointeger(L, 2) + 1);
	if (stmt->old_tuple != NULL)
		luaT_pushtuple(L, stmt->old_tuple);
	else
		lua_pushnil(L);
	if (stmt->new_tuple != NULL)
		luaT_pushtuple(L, stmt->new_tuple);
	else
		lua_pushnil(L);
	lua_pushinteger(L, space_id(stmt->space));
	/* Prepare a statement to the next call. */
	stmt = stailq_next_entry(stmt, next);
	lua_pushlightuserdata(L, stmt);
	lua_replace(L, lua_upvalueindex(2));
	return 4;
}

/**
 * Open an iterator over the transaction statements. This is a C
 * closure and 1 upvalue should be available - id of the
 * transaction to iterate over.
 * It returns 3 values which can be used in Lua 'for': iterator
 * generator function, unused nil and the zero key.
 */
static int
lbox_txn_pairs(struct lua_State *L)
{
	int64_t txn_id = luaL_toint64(L, lua_upvalueindex(1));
	struct txn *txn = in_txn();
	if (txn == NULL || txn->id != txn_id) {
		diag_set(ClientError, ER_CURSOR_NO_TRANSACTION);
		return luaT_error(L);
	}
	luaL_pushint64(L, txn_id);
	lua_pushlightuserdata(L, stailq_first_entry(&txn->stmts,
						    struct txn_stmt, next));
	lua_pushcclosure(L, lbox_txn_iterator_next, 2);
	lua_pushnil(L);
	lua_pushinteger(L, 0);
	return 3;
}

/**
 * Push an argument for on_commit Lua trigger. The argument is
 * a function to open an iterator over the transaction statements.
 */
static int
lbox_push_txn(struct lua_State *L, void *event)
{
	struct txn *txn = (struct txn *) event;
	luaL_pushint64(L, txn->id);
	lua_pushcclosure(L, lbox_txn_pairs, 1);
	return 1;
}

/**
 * Update the transaction on_commit/rollback triggers.
 * @sa lbox_trigger_reset.
 */
#define LBOX_TXN_TRIGGER(name)                                                 \
static int                                                                     \
lbox_on_##name(struct lua_State *L) {                                          \
	struct txn *txn = in_txn();                                            \
	int top = lua_gettop(L);                                               \
	if (top > 2 || txn == NULL) {                                          \
		return luaL_error(L, "Usage inside a transaction: "            \
				  "box.on_" #name "([function | nil, "         \
				  "[function | nil]])");                       \
	}                                                                      \
	txn_init_triggers(txn);                                                \
	return lbox_trigger_reset(L, 2, &txn->on_##name, lbox_push_txn, NULL); \
}

LBOX_TXN_TRIGGER(commit)
LBOX_TXN_TRIGGER(rollback)

static int
lbox_snapshot(struct lua_State *L)
{
	int ret = box_checkpoint();
	if (ret == 0) {
		lua_pushstring(L, "ok");
		return 1;
	}
	return luaT_error(L);
}

/** Argument passed to lbox_backup_fn(). */
struct lbox_backup_arg {
	/** Lua state. */
	struct lua_State *L;
	/** Number of files in the resulting table. */
	int file_count;
};

static int
lbox_backup_cb(const char *path, void *cb_arg)
{
	struct lbox_backup_arg *arg = cb_arg;
	lua_pushinteger(arg->L, ++arg->file_count);
	lua_pushstring(arg->L, path);
	lua_settable(arg->L, -3);
	return 0;
}

static int
lbox_backup_start(struct lua_State *L)
{
	int checkpoint_idx = 0;
	if (lua_gettop(L) > 0) {
		checkpoint_idx = luaL_checkint(L, 1);
		if (checkpoint_idx < 0)
			return luaL_error(L, "invalid checkpoint index");
	}
	lua_newtable(L);
	struct lbox_backup_arg arg = {
		.L = L,
	};
	if (box_backup_start(checkpoint_idx, lbox_backup_cb, &arg) != 0)
		return luaT_error(L);
	return 1;
}

static int
lbox_backup_stop(struct lua_State *L)
{
	(void)L;
	box_backup_stop();
	return 0;
}

static const struct luaL_Reg boxlib[] = {
	{"commit", lbox_commit},
	{"rollback", lbox_rollback},
	{"on_commit", lbox_on_commit},
	{"on_rollback", lbox_on_rollback},
	{"snapshot", lbox_snapshot},
	{NULL, NULL}
};

static const struct luaL_Reg boxlib_backup[] = {
	{"start", lbox_backup_start},
	{"stop", lbox_backup_stop},
	{NULL, NULL}
};

#include "say.h"

void
box_lua_init(struct lua_State *L)
{
	/* Use luaL_register() to set _G.box */
	luaL_register(L, "box", boxlib);
	lua_pop(L, 1);

	luaL_register(L, "box.backup", boxlib_backup);
	lua_pop(L, 1);

	box_lua_error_init(L);
	box_lua_tuple_init(L);
	box_lua_call_init(L);
	box_lua_cfg_init(L);
	box_lua_slab_init(L);
	box_lua_index_init(L);
	box_lua_space_init(L);
	box_lua_sequence_init(L);
	box_lua_misc_init(L);
	box_lua_info_init(L);
	box_lua_stat_init(L);
	box_lua_ctl_init(L);
	box_lua_session_init(L);
	box_lua_xlog_init(L);
	box_lua_sqlite_init(L);
	luaopen_net_box(L);
	lua_pop(L, 1);
	tarantool_lua_console_init(L);
	lua_pop(L, 1);
	luaopen_merger(L);
	lua_pop(L, 1);

	/* Load Lua extension */
	for (const char **s = lua_sources; *s; s += 2) {
		const char *modname = *s;
		const char *modsrc = *(s + 1);
		const char *modfile = lua_pushfstring(L,
			"@builtin/%s.lua", modname);
		if (luaL_loadbuffer(L, modsrc, strlen(modsrc), modfile))
			panic("Error loading Lua module %s...: %s",
			      modname, lua_tostring(L, -1));
		lua_call(L, 0, 0);
		lua_pop(L, 1); /* modfile */
	}

	assert(lua_gettop(L) == 0);
}
