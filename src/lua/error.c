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

#include <diag.h>
#include <fiber.h>
#include <backtrace.h>
#include "utils.h"
#include "error.h"

static int CTID_CONST_STRUCT_ERROR_REF = 0;

int
luaT_error(lua_State *L)
{
	struct error *e = diag_last_error(&fiber()->diag);
	assert(e != NULL);
	error_ref(e);
	/*
	 * gh-1955 luaT_pusherror allocates Lua objects, thus it may trigger
	 * GC. GC may invoke finalizers which are arbitrary Lua code,
	 * potentially invalidating last error object, hence error_ref
	 * below.
	 */
	luaT_pusherror(L, e);
	error_unref(e);
	lua_error(L);
	unreachable();
	return 0;
}

struct error *
luaL_iserror(struct lua_State *L, int narg)
{
	assert(CTID_CONST_STRUCT_ERROR_REF != 0);
	if (lua_type(L, narg) != LUA_TCDATA)
		return NULL;

	uint32_t ctypeid;
	void *data = luaL_checkcdata(L, narg, &ctypeid);
	if (ctypeid != (uint32_t) CTID_CONST_STRUCT_ERROR_REF)
		return NULL;

	struct error *e = *(struct error **) data;
	assert(e->refs);
	return e;
}

static struct error *
luaL_checkerror(struct lua_State *L, int narg)
{
	struct error *error = luaL_iserror(L, narg);
	if (error == NULL)  {
		luaL_error(L, "Invalid argument #%d (error expected, got %s)",
			   narg, lua_typename(L, lua_type(L, narg)));
	}
	return error;
}

static int
luaL_error_gc(struct lua_State *L)
{
	struct error *error = luaL_checkerror(L, 1);
	error_unref(error);
	return 0;
}

void
luaT_pusherror(struct lua_State *L, struct error *e)
{
	assert(CTID_CONST_STRUCT_ERROR_REF != 0);
	struct error **ptr = (struct error **)
		luaL_pushcdata(L, CTID_CONST_STRUCT_ERROR_REF);
	*ptr = e;
	/* The order is important - first reference the error, then set gc */
	error_ref(e);
	lua_pushcfunction(L, luaL_error_gc);
	luaL_setcdatagc(L, -2);
}

static inline void
copy_frame(struct diag_frame *dest, struct diag_frame *src)
{
	dest->line = src->line;
	strcpy(dest->func_name, src->func_name);
	strcpy(dest->filename, src->filename);
}

static int
traceback_error(struct lua_State *L, struct error *e)
{
	lua_Debug ar;
	int level = 0;
	struct rlist lua_frames;
	struct rlist *lua_framesp;
	struct diag_frame *frame, *next;
	if (e->frames_count <= 0) {
		lua_framesp = &e->frames;
	} else {
		lua_framesp = &lua_frames;
		rlist_create(lua_framesp);
	}
	/*
	 * At this moment error object was created from exception so
	 * traceback list is already created and filled with C trace.
	 * Now we need to create lua trace and merge it with the existing one.
	 */
	while (lua_getstack(L, level++, &ar) > 0) {
		lua_getinfo(L, "Sln", &ar);
		frame = (struct diag_frame *) malloc(sizeof(*frame));
		if (frame == NULL) {
			luaT_pusherror(L, e);
			return 1;
		}
		if (*ar.what == 'L' || *ar.what == 'm') {
			strcpy(frame->filename, ar.short_src);
			frame->line = ar.currentline;
			if (*ar.namewhat != '\0') {
				strcpy(frame->func_name, ar.name);
			} else {
				frame->func_name[0] = 0;
			}
			e->frames_count++;
		} else if (*ar.what == 'C') {
			if (*ar.namewhat != '\0') {
				strcpy(frame->func_name, ar.name);
			} else {
				frame->func_name[0] = 0;
			}
			frame->filename[0] = 0;
			frame->line = 0;
			e->frames_count++;
		}
		rlist_add_entry(lua_framesp, frame, link);
	}
	if (e->frames_count > 0) {
		struct diag_frame *lua_frame = rlist_first_entry(lua_framesp,
							 typeof(*lua_frame),
							 link);
		rlist_foreach_entry(frame, &e->frames, link) {
			/* We insert trace of lua user code in c trace,
			 * where C calls lua.*/
			if (strncmp(frame->func_name, "lj_BC_FUNCC",
				    sizeof("lj_BC_FUNCC") - 1) == 0 &&
			    frame->line != -1) {
				/* We have to bypass internal error calls. */
				next = rlist_next_entry(frame, link);
				if (strncmp(next->func_name, "lj_err_run",
					    sizeof("lj_err_run") - 1) == 0)
					continue;
				e->frames_count--;
				for (;&lua_frame->link != lua_framesp;
				      lua_frame = rlist_next_entry(lua_frame,
								    link)) {
					/* Skip empty frames. */
					if (strncmp(lua_frame->filename,
						    "[C]",
						    sizeof("[C]") - 1) == 0 &&
					    (*lua_frame->func_name) == '?')
						continue;
					break;
				}

				for (; &lua_frame->link != lua_framesp;
				       lua_frame = rlist_next_entry(lua_frame,
								    link)) {
					if (lua_frame->filename[0]== 0 &&
					    lua_frame->func_name[0] == 0)
						break;
					struct diag_frame *frame_copy =
						(struct diag_frame *)
						    malloc(sizeof(*frame_copy));
					copy_frame(frame_copy, lua_frame);
					rlist_add_entry(&frame->link,
							frame_copy, link);
					e->frames_count++;
					/*
					 * Mark the C frame that it was replaced
					 * with lua.
					 */
					frame->line = -1;
				}
			}
		}
	}
	if (&e->frames != lua_framesp) {
		/* Cleanup lua trace. */
		for (frame = rlist_first_entry(lua_framesp, typeof(*frame),
					       link);
		     &frame->link != lua_framesp;) {
			next = rlist_next_entry(frame, link);
			free(frame);
			frame = next;
		}
	}
	luaT_pusherror(L, e);
	return 1;
}

int
luaT_traceback(struct lua_State *L)
{
	struct error* e = luaL_iserror(L, -1);
	if (e == NULL) {
		const char *msg = lua_tostring(L, -1);
		if (msg == NULL) {
			say_error("pcall calls error handler on empty error");
			return 0;
		} else {
			e = BuildLuajitError(__FILE__, __LINE__, msg);
		}
	}
	return traceback_error(L, e);
}

int
lua_error_gettraceback(struct lua_State *L)
{
	struct error *e = luaL_iserror(L, -1);
	if (!e) {
		return 0;
	}
	lua_newtable(L);
	if (e->frames_count <= 0) {
		return 1;
	}
	struct diag_frame *frame;
	int index = 1;
	rlist_foreach_entry(frame, &e->frames, link) {
		if (frame->func_name[0] != 0 || frame->line > 0 ||
		    frame->filename[0] != 0) {
			if (strncmp(frame->func_name, "lj_BC_FUNCC",
				    sizeof("lj_BC_FUNCC") - 1) == 0 &&
			    frame->line == -1)
				continue;
			/* push index */
			lua_pushnumber(L, index++);
			/* push value - table of filename and line */
			lua_newtable(L);
			if (frame->func_name[0] != 0) {
				lua_pushstring(L, "function");
				lua_pushstring(L, frame->func_name);
				lua_settable(L, -3);
			}
			if (frame->filename[0] != 0) {
				lua_pushstring(L, "file");
				lua_pushstring(L, frame->filename);
				lua_settable(L, -3);
			}
			if (frame->line > 0) {
				lua_pushstring(L, "line");
				lua_pushinteger(L, frame->line);
				lua_settable(L, -3);
			}
			lua_settable(L, -3);
		}
	}
	return 1;
}

/**
 * Function replacing lua pcall function.
 * We handle lua errors, creating tarantool error objects and
 * saving traceback inside.
 */
static int
luaB_pcall(struct lua_State *L)
{
	int status;
	luaL_checkany(L, 1);
	status = luaT_call(L, lua_gettop(L) - 1, LUA_MULTRET);
	lua_pushboolean(L, (status == 0));
	lua_insert(L, 1);
	return lua_gettop(L);  /* return status + all results */
}

/**
 * Function replacing lua error function.
 * We have to handle tarantool error objects, converting them to string
 * for generating string errors with path in case of call error(msg, level),
 * where level > 0.
 */
static int
luaB_error (lua_State *L) {
	int level = luaL_optint(L, 2, 1);
	lua_settop(L, 1);
	if (lua_type(L, 1) == LUA_TCDATA) {
		assert(CTID_CONST_STRUCT_ERROR_REF != 0);
		uint32_t ctypeid;
		void *data = luaL_checkcdata(L, 1, &ctypeid);
		if (ctypeid != (uint32_t) CTID_CONST_STRUCT_ERROR_REF)
			return lua_error(L);

		struct error *e = *(struct error **) data;
		lua_pushstring(L, e->errmsg);
	}
	if (lua_isstring(L, -1) && level > 0) {  /* add extra information? */
		luaL_where(L, level);
		lua_insert(L, lua_gettop(L) - 1);
		lua_concat(L, 2);
	}
	return lua_error(L);
}

void
tarantool_lua_error_init(struct lua_State *L)
{

	/* Get CTypeID for `struct error *' */
	int rc = luaL_cdef(L, "struct error;");
	assert(rc == 0);
	(void) rc;
	CTID_CONST_STRUCT_ERROR_REF = luaL_ctypeid(L, "const struct error &");
	assert(CTID_CONST_STRUCT_ERROR_REF != 0);
	lua_pushcfunction(L, luaB_pcall);
	lua_setglobal(L, "pcall");
	lua_pushcfunction(L, luaB_error);
	lua_setglobal(L, "error");

	static const luaL_Reg errorslib[] = {
		{"get_traceback", lua_error_gettraceback},
		{ NULL, NULL}
	};
	luaL_register_module(L, "error", errorslib);
	lua_pop(L, 1);
}
