/*
 * Copyright 2010-2015, Tarantool AUTHORS, please see AUTHORS file.
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
#include "lua/msgpack.h"
#include "mpstream.h"
#include "lua/utils.h"

#if defined(LUAJIT)
#include <lj_ctype.h>
#endif /* defined(LUAJIT) */
#include <lauxlib.h> /* struct luaL_error */

#include <msgpuck.h>
#include <small/region.h>
#include <small/ibuf.h>

#include <fiber.h>

void
luamp_error(void *error_ctx)
{
	struct lua_State *L = (struct lua_State *) error_ctx;
	luaL_error(L, diag_last_error(diag_get())->errmsg);
}

static uint32_t CTID_CHAR_PTR;
static uint32_t CTID_STRUCT_IBUF;

struct luaL_serializer *luaL_msgpack_default = NULL;

static enum mp_type
luamp_encode_extension_default(struct lua_State *L, int idx,
			       struct mpstream *stream);

static void
luamp_decode_extension_default(struct lua_State *L, const char **data);

static luamp_encode_extension_f luamp_encode_extension =
		luamp_encode_extension_default;
static luamp_decode_extension_f luamp_decode_extension =
		luamp_decode_extension_default;

static enum mp_type
luamp_encode_extension_default(struct lua_State *L, int idx,
			       struct mpstream *stream)
{
	(void) L;
	(void) idx;
	(void) stream;
	return MP_EXT;
}

void
luamp_set_encode_extension(luamp_encode_extension_f handler)
{
	if (handler == NULL) {
		luamp_encode_extension = luamp_encode_extension_default;
	} else {
		luamp_encode_extension = handler;
	}
}

static void
luamp_decode_extension_default(struct lua_State *L, const char **data)
{
	luaL_error(L, "msgpack.decode: unsupported extension: %u",
		   (unsigned char) **data);
}


void
luamp_set_decode_extension(luamp_decode_extension_f handler)
{
	if (handler == NULL) {
		luamp_decode_extension = luamp_decode_extension_default;
	} else {
		luamp_decode_extension = handler;
	}
}

enum mp_type
luamp_encode_r(struct lua_State *L, struct luaL_serializer *cfg,
	       struct mpstream *stream, struct luaL_field *field,
	       int level)
{
	int top = lua_gettop(L);
	enum mp_type type;

restart: /* used by MP_EXT */
	switch (field->type) {
	case MP_UINT:
		mpstream_encode_uint(stream, field->ival);
		return MP_UINT;
	case MP_STR:
		mpstream_encode_strn(stream, field->sval.data, field->sval.len);
		return MP_STR;
	case MP_BIN:
		mpstream_encode_strn(stream, field->sval.data, field->sval.len);
		return MP_BIN;
	case MP_INT:
		mpstream_encode_int(stream, field->ival);
		return MP_INT;
	case MP_FLOAT:
		mpstream_encode_float(stream, field->fval);
		return MP_FLOAT;
	case MP_DOUBLE:
		mpstream_encode_double(stream, field->dval);
		return MP_DOUBLE;
	case MP_BOOL:
		mpstream_encode_bool(stream, field->bval);
		return MP_BOOL;
	case MP_NIL:
		mpstream_encode_nil(stream);
		return MP_NIL;
	case MP_MAP:
		/* Map */
		if (level >= cfg->encode_max_depth) {
			mpstream_encode_nil(stream); /* Limit nested maps */
			return MP_NIL;
		}
		mpstream_encode_map(stream, field->size);
		lua_pushnil(L);  /* first key */
		while (lua_next(L, top) != 0) {
			lua_pushvalue(L, -2); /* push a copy of key to top */
			if (luaL_tofield(L, cfg, lua_gettop(L), field) < 0)
				luaT_error(L);
			luamp_encode_r(L, cfg, stream, field, level + 1);
			lua_pop(L, 1); /* pop a copy of key */
			if (luaL_tofield(L, cfg, lua_gettop(L), field) < 0)
				luaT_error(L);
			luamp_encode_r(L, cfg, stream, field, level + 1);
			lua_pop(L, 1); /* pop value */
		}
		assert(lua_gettop(L) == top);
		return MP_MAP;
	case MP_ARRAY:
		/* Array */
		if (level >= cfg->encode_max_depth) {
			mpstream_encode_nil(stream); /* Limit nested arrays */
			return MP_NIL;
		}
		uint32_t size = field->size;
		mpstream_encode_array(stream, size);
		for (uint32_t i = 0; i < size; i++) {
			lua_rawgeti(L, top, i + 1);
			if (luaL_tofield(L, cfg, top + 1, field) < 0)
				luaT_error(L);
			luamp_encode_r(L, cfg, stream, field, level + 1);
			lua_pop(L, 1);
		}
		assert(lua_gettop(L) == top);
		return MP_ARRAY;
	case MP_EXT:
		/* Run trigger if type can't be encoded */
		type = luamp_encode_extension(L, top, stream);
		if (type != MP_EXT)
			return type; /* Value has been packed by the trigger */
		/* Try to convert value to serializable type */
		luaL_convertfield(L, cfg, top, field);
		/* handled by luaL_convertfield */
		assert(field->type != MP_EXT);
		assert(lua_gettop(L) == top);
		goto restart;
	}
	return MP_EXT;
}

enum mp_type
luamp_encode(struct lua_State *L, struct luaL_serializer *cfg,
	     struct mpstream *stream, int index)
{
	int top = lua_gettop(L);
	if (index < 0)
		index = top + index + 1;

	bool on_top = (index == top);
	if (!on_top) {
		lua_pushvalue(L, index); /* copy a value to the stack top */
	}

	struct luaL_field field;
	if (luaL_tofield(L, cfg, lua_gettop(L), &field) < 0)
		luaT_error(L);
	enum mp_type top_type = luamp_encode_r(L, cfg, stream, &field, 0);

	if (!on_top) {
		lua_remove(L, top + 1); /* remove a value copy */
	}

	return top_type;
}

void
luamp_decode(struct lua_State *L, struct luaL_serializer *cfg,
	     const char **data)
{
	double d;
	switch (mp_typeof(**data)) {
	case MP_UINT:
		luaL_pushuint64(L, mp_decode_uint(data));
		break;
	case MP_INT:
		luaL_pushint64(L, mp_decode_int(data));
		break;
	case MP_FLOAT:
		d = mp_decode_float(data);
		luaL_checkfinite(L, cfg, d);
		lua_pushnumber(L, d);
		return;
	case MP_DOUBLE:
		d = mp_decode_double(data);
		luaL_checkfinite(L, cfg, d);
		lua_pushnumber(L, d);
		return;
	case MP_STR:
	{
		uint32_t len = 0;
		const char *str = mp_decode_str(data, &len);
		lua_pushlstring(L, str, len);
		return;
	}
	case MP_BIN:
	{
		uint32_t len = 0;
		const char *str = mp_decode_bin(data, &len);
		lua_pushlstring(L, str, len);
		return;
	}
	case MP_BOOL:
		lua_pushboolean(L, mp_decode_bool(data));
		return;
	case MP_NIL:
		mp_decode_nil(data);
		luaL_pushnull(L);
		return;
	case MP_ARRAY:
	{
		uint32_t size = mp_decode_array(data);
		lua_createtable(L, size, 0);
		for (uint32_t i = 0; i < size; i++) {
			luamp_decode(L, cfg, data);
			lua_rawseti(L, -2, i + 1);
		}
		if (cfg->decode_save_metatables)
			luaL_setarrayhint(L, -1);
		return;
	}
	case MP_MAP:
	{
		uint32_t size = mp_decode_map(data);
		lua_createtable(L, 0, size);
		for (uint32_t i = 0; i < size; i++) {
			luamp_decode(L, cfg, data);
			luamp_decode(L, cfg, data);
			lua_settable(L, -3);
		}
		if (cfg->decode_save_metatables)
			luaL_setmaphint(L, -1);
		return;
	}
	case MP_EXT:
		luamp_decode_extension(L, data);
		break;
	}
}


static int
lua_msgpack_encode(lua_State *L)
{
	int index = lua_gettop(L);
	if (index < 1)
		return luaL_error(L, "msgpack.encode: a Lua object expected");

	struct ibuf *buf;
	if (index > 1) {
		uint32_t ctypeid;
		buf = luaL_checkcdata(L, 2, &ctypeid);
		if (ctypeid != CTID_STRUCT_IBUF)
			return luaL_error(L, "msgpack.encode: argument 2 "
					  "must be of type 'struct ibuf'");
	} else {
		buf = tarantool_lua_ibuf;
		ibuf_reset(buf);
	}
	size_t used = ibuf_used(buf);

	struct luaL_serializer *cfg = luaL_checkserializer(L);

	struct mpstream stream;
	mpstream_init(&stream, buf, ibuf_reserve_cb, ibuf_alloc_cb,
		      luamp_error, L);

	luamp_encode(L, cfg, &stream, 1);
	mpstream_flush(&stream);

	if (index > 1) {
		lua_pushinteger(L, ibuf_used(buf) - used);
	} else {
		lua_pushlstring(L, buf->buf, ibuf_used(buf));
		ibuf_reinit(buf);
	}
	return 1;
}

static int
lua_msgpack_decode_cdata(lua_State *L, bool check)
{
	uint32_t ctypeid;
	const char *data = *(const char **)luaL_checkcdata(L, 1, &ctypeid);
	if (ctypeid != CTID_CHAR_PTR) {
		return luaL_error(L, "msgpack.decode: "
				  "a Lua string or 'char *' expected");
	}
	if (check) {
		size_t data_len = luaL_checkinteger(L, 2);
		const char *p = data;
		if (mp_check(&p, data + data_len) != 0)
			return luaL_error(L, "msgpack.decode: invalid MsgPack");
	}
	struct luaL_serializer *cfg = luaL_checkserializer(L);
	luamp_decode(L, cfg, &data);
	*(const char **)luaL_pushcdata(L, ctypeid) = data;
	return 2;
}

static int
lua_msgpack_decode_string(lua_State *L, bool check)
{
	ptrdiff_t offset = 0;
	size_t data_len;
	const char *data = lua_tolstring(L, 1, &data_len);
	if (lua_gettop(L) > 1) {
		offset = luaL_checkinteger(L, 2) - 1;
		if (offset < 0 || (size_t)offset >= data_len)
			return luaL_error(L, "msgpack.decode: "
					  "offset is out of bounds");
	}
	if (check) {
		const char *p = data + offset;
		if (mp_check(&p, data + data_len) != 0)
			return luaL_error(L, "msgpack.decode: invalid MsgPack");
	}
	struct luaL_serializer *cfg = luaL_checkserializer(L);
	const char *p = data + offset;
	luamp_decode(L, cfg, &p);
	lua_pushinteger(L, p - data + 1);
	return 2;
}

static int
lua_msgpack_decode(lua_State *L)
{
	int index = lua_gettop(L);
	int type = index >= 1 ? lua_type(L, 1) : LUA_TNONE;
	switch (type) {
	case LUA_TCDATA:
		return lua_msgpack_decode_cdata(L, true);
	case LUA_TSTRING:
		return lua_msgpack_decode_string(L, true);
	default:
		return luaL_error(L, "msgpack.decode: "
				  "a Lua string or 'char *' expected");
	}
}

static int
lua_msgpack_decode_unchecked(lua_State *L)
{
	int index = lua_gettop(L);
	int type = index >= 1 ? lua_type(L, 1) : LUA_TNONE;
	switch (type) {
	case LUA_TCDATA:
		return lua_msgpack_decode_cdata(L, false);
	case LUA_TSTRING:
		return lua_msgpack_decode_string(L, false);
	default:
		return luaL_error(L, "msgpack.decode: "
				  "a Lua string or 'char *' expected");
	}
}

static int
lua_ibuf_msgpack_decode(lua_State *L)
{
	uint32_t ctypeid = 0;
	const char *rpos = *(const char **)luaL_checkcdata(L, 1, &ctypeid);
	if (rpos == NULL) {
		luaL_error(L, "msgpack.ibuf_decode: rpos is null");
	}
	struct luaL_serializer *cfg = luaL_checkserializer(L);
	luamp_decode(L, cfg, &rpos);
	*(const char **)luaL_pushcdata(L, ctypeid) = rpos;
	lua_pushvalue(L, -2);
	return 2;
}

static int
lua_msgpack_new(lua_State *L);

static const luaL_Reg msgpacklib[] = {
	{ "encode", lua_msgpack_encode },
	{ "decode", lua_msgpack_decode },
	{ "decode_unchecked", lua_msgpack_decode_unchecked },
	{ "ibuf_decode", lua_ibuf_msgpack_decode },
	{ "new", lua_msgpack_new },
	{ NULL, NULL }
};

static int
lua_msgpack_new(lua_State *L)
{
	luaL_newserializer(L, NULL, msgpacklib);
	return 1;
}

LUALIB_API int
luaopen_msgpack(lua_State *L)
{
	int rc = luaL_cdef(L, "struct ibuf;");
	assert(rc == 0);
	(void) rc;
	CTID_STRUCT_IBUF = luaL_ctypeid(L, "struct ibuf");
	assert(CTID_STRUCT_IBUF != 0);
	CTID_CHAR_PTR = luaL_ctypeid(L, "char *");
	assert(CTID_CHAR_PTR != 0);
	luaL_msgpack_default = luaL_newserializer(L, "msgpack", msgpacklib);
	return 1;
}
