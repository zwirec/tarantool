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
#include "lua/utils.h"

#include <assert.h>
#include <errno.h>

#include <trivia/util.h>
#include <diag.h>
#include <fiber.h>

#include "box/error.h"

int luaL_nil_ref = LUA_REFNIL;
int luaL_map_metatable_ref = LUA_REFNIL;
int luaL_array_metatable_ref = LUA_REFNIL;

void *
luaL_pushcdata(struct lua_State *L, uint32_t ctypeid)
{
	/*
	 * ctypeid is actually has CTypeID type.
	 * CTypeId is defined somewhere inside luajit's internal
	 * headers.
	 */
	assert(sizeof(ctypeid) == sizeof(CTypeID));

	/* Code below is based on ffi_new() from luajit/src/lib_ffi.c */

	/* Get information about ctype */
	CTSize size;
	CTState *cts = ctype_cts(L);
	CTInfo info = lj_ctype_info(cts, ctypeid, &size);
	assert(size != CTSIZE_INVALID);

	/* Allocate a new cdata */
	GCcdata *cd = lj_cdata_new(cts, ctypeid, size);

	/* Anchor the uninitialized cdata with the stack. */
	TValue *o = L->top;
	setcdataV(L, o, cd);
	incr_top(L);

	/*
	 * lj_cconv_ct_init is omitted for non-structs because it actually
	 * does memset()
	 * Caveats: cdata memory is returned uninitialized
	 */
	if (ctype_isstruct(info)) {
		/* Initialize cdata. */
		CType *ct = ctype_raw(cts, ctypeid);
		lj_cconv_ct_init(cts, ct, size, cdataptr(cd), o,
				 (MSize)(L->top - o));
		/* Handle ctype __gc metamethod. Use the fast lookup here. */
		cTValue *tv = lj_tab_getinth(cts->miscmap, -(int32_t)ctypeid);
		if (tv && tvistab(tv) && (tv = lj_meta_fast(L, tabV(tv), MM_gc))) {
			GCtab *t = cts->finalizer;
			if (gcref(t->metatable)) {
				/* Add to finalizer table, if still enabled. */
				copyTV(L, lj_tab_set(L, t, o), tv);
				lj_gc_anybarriert(L, t);
				cd->marked |= LJ_GC_CDATA_FIN;
			}
		}
	}

	lj_gc_check(L);
	return cdataptr(cd);
}

void *
luaL_checkcdata(struct lua_State *L, int idx, uint32_t *ctypeid)
{
	/* Calculate absolute value in the stack. */
	if (idx < 0)
		idx = lua_gettop(L) + idx + 1;

	if (lua_type(L, idx) != LUA_TCDATA) {
		*ctypeid = 0;
		luaL_error(L, "expected cdata as %d argument", idx);
		return NULL;
	}

	GCcdata *cd = cdataV(L->base + idx - 1);
	*ctypeid = cd->ctypeid;
	return (void *)cdataptr(cd);
}

uint32_t
luaL_ctypeid(struct lua_State *L, const char *ctypename)
{
	int idx = lua_gettop(L);
	/* This function calls ffi.typeof to determine CDataType */

	/* Get ffi.typeof function */
	luaL_loadstring(L, "return require('ffi').typeof");
	lua_call(L, 0, 1);
	/* FFI must exist */
	assert(lua_gettop(L) == idx + 1 && lua_isfunction(L, idx + 1));
	/* Push the first argument to ffi.typeof */
	lua_pushstring(L, ctypename);
	/* Call ffi.typeof() */
	lua_call(L, 1, 1);
	/* Returned type must be LUA_TCDATA with CTID_CTYPEID */
	uint32_t ctypetypeid;
	CTypeID ctypeid = *(CTypeID *)luaL_checkcdata(L, idx + 1, &ctypetypeid);
	assert(ctypetypeid == CTID_CTYPEID);

	lua_settop(L, idx);
	return ctypeid;
}

int
luaL_cdef(struct lua_State *L, const char *what)
{
	int idx = lua_gettop(L);
	(void) idx;
	/* This function calls ffi.cdef  */

	/* Get ffi.typeof function */
	luaL_loadstring(L, "return require('ffi').cdef");
	lua_call(L, 0, 1);
	/* FFI must exist */
	assert(lua_gettop(L) == idx + 1 && lua_isfunction(L, idx + 1));
	/* Push the argument to ffi.cdef */
	lua_pushstring(L, what);
	/* Call ffi.cdef() */
	return lua_pcall(L, 1, 0, 0);
}

void
luaL_setcdatagc(struct lua_State *L, int idx)
{
	/* Calculate absolute value in the stack. */
	if (idx < 0)
		idx = lua_gettop(L) + idx + 1;

	/* Code below is based on ffi_gc() from luajit/src/lib_ffi.c */

	/* Get cdata from the stack */
	assert(lua_type(L, idx) == LUA_TCDATA);
	GCcdata *cd = cdataV(L->base + idx - 1);

	/* Get finalizer from the stack */
	TValue *fin = lj_lib_checkany(L, lua_gettop(L));

#if !defined(NDEBUG)
	CTState *cts = ctype_cts(L);
	CType *ct = ctype_raw(cts, cd->ctypeid);
	(void) ct;
	assert(ctype_isptr(ct->info) || ctype_isstruct(ct->info) ||
	       ctype_isrefarray(ct->info));
#endif /* !defined(NDEBUG) */

	/* Set finalizer */
	lj_cdata_setfin(L, cd, gcval(fin), itype(fin));

	/* Pop finalizer */
	lua_pop(L, 1);
}


#define OPTION(type, name, defvalue) { #name, \
	offsetof(struct luaL_serializer, name), type, defvalue}
/**
 * Configuration options for serializers
 * @sa struct luaL_serializer
 */
static struct {
	const char *name;
	size_t offset; /* offset in structure */
	int type;
	int defvalue;
} OPTIONS[] = {
	OPTION(LUA_TBOOLEAN, encode_sparse_convert, 1),
	OPTION(LUA_TNUMBER,  encode_sparse_ratio, 2),
	OPTION(LUA_TNUMBER,  encode_sparse_safe, 10),
	OPTION(LUA_TNUMBER,  encode_max_depth, 32),
	OPTION(LUA_TBOOLEAN, encode_invalid_numbers, 1),
	OPTION(LUA_TNUMBER,  encode_number_precision, 14),
	OPTION(LUA_TBOOLEAN, encode_load_metatables, 1),
	OPTION(LUA_TBOOLEAN, encode_use_tostring, 0),
	OPTION(LUA_TBOOLEAN, encode_invalid_as_nil, 0),
	OPTION(LUA_TBOOLEAN, decode_invalid_numbers, 1),
	OPTION(LUA_TBOOLEAN, decode_save_metatables, 1),
	OPTION(LUA_TNUMBER,  decode_max_depth, 32),
	{ NULL, 0, 0, 0},
};

/**
 * Configure one field in @a cfg.
 * @param L Lua stack.
 * @param i Index of option in OPTIONS[].
 * @param cfg Serializer to inherit configuration.
 * @retval Pointer to the value of option.
 * @retval NULL if option is not in the table.
 */
static int *
luaL_serializer_parse_option(struct lua_State *L, int i,
			     struct luaL_serializer *cfg)
{
	lua_getfield(L, 2, OPTIONS[i].name);
	if (lua_isnil(L, -1)) {
		lua_pop(L, 1);
		return NULL;
	}
	/*
	 * Update struct luaL_serializer using pointer to a
	 * configuration value (all values must be `int` for that).
	*/
	int *pval = (int *) ((char *) cfg + OPTIONS[i].offset);
	switch (OPTIONS[i].type) {
	case LUA_TBOOLEAN:
		*pval = lua_toboolean(L, -1);
		break;
	case LUA_TNUMBER:
		*pval = lua_tointeger(L, -1);
		break;
	default:
		unreachable();
	}
	lua_pop(L, 1);
	return pval;
}

void
luaL_serializer_parse_options(struct lua_State *L,
			      struct luaL_serializer *cfg)
{
	for (int i = 0; OPTIONS[i].name != NULL; i++)
		luaL_serializer_parse_option(L, i, cfg);
}

/**
 * @brief serializer.cfg{} Lua binding for serializers.
 * serializer.cfg is a table that contains current configuration values from
 * luaL_serializer structure. serializer.cfg has overriden __call() method
 * to change configuration keys in internal userdata (like box.cfg{}).
 * Please note that direct change in serializer.cfg.key will not affect
 * internal state of userdata.
 * @param L lua stack
 * @return 0
 */
static int
luaL_serializer_cfg(struct lua_State *L)
{
	luaL_checktype(L, 1, LUA_TTABLE); /* serializer */
	luaL_checktype(L, 2, LUA_TTABLE); /* serializer.cfg */
	luaL_serializer_parse_options(L, luaL_checkserializer(L));
	return 0;
}

/**
 * @brief serializer.new() Lua binding.
 * @param L stack
 * @param reg methods to register
 * @param parent parent serializer to inherit configuration
 * @return new serializer
 */
struct luaL_serializer *
luaL_newserializer(struct lua_State *L, const char *modname, const luaL_Reg *reg)
{
	luaL_checkstack(L, 1, "too many upvalues");

	/* Create new module */
	lua_newtable(L);

	/* Create new configuration */
	struct luaL_serializer *serializer = (struct luaL_serializer *)
			lua_newuserdata(L, sizeof(*serializer));
	luaL_getmetatable(L, LUAL_SERIALIZER);
	lua_setmetatable(L, -2);
	memset(serializer, 0, sizeof(*serializer));

	for (; reg->name != NULL; reg++) {
		/* push luaL_serializer as upvalue */
		lua_pushvalue(L, -1);
		/* register method */
		lua_pushcclosure(L, reg->func, 1);
		lua_setfield(L, -3, reg->name);
	}

	/* Add cfg{} */
	lua_newtable(L); /* cfg */
	lua_newtable(L); /* metatable */
	lua_pushvalue(L, -3); /* luaL_serializer */
	lua_pushcclosure(L, luaL_serializer_cfg, 1);
	lua_setfield(L, -2, "__call");
	lua_setmetatable(L, -2);
	/* Save configuration values to serializer.cfg */
	for (int i = 0; OPTIONS[i].name != NULL; i++) {
		int *pval = (int *) ((char *) serializer + OPTIONS[i].offset);
		*pval = OPTIONS[i].defvalue;
		switch (OPTIONS[i].type) {
		case LUA_TBOOLEAN:
			lua_pushboolean(L, *pval);
			break;
		case LUA_TNUMBER:
			lua_pushinteger(L, *pval);
			break;
		default:
			unreachable();
		}
		lua_setfield(L, -2, OPTIONS[i].name);
	}
	lua_setfield(L, -3, "cfg");

	lua_pop(L, 1);  /* remove upvalues */

	luaL_pushnull(L);
	lua_setfield(L, -2, "NULL");
	lua_rawgeti(L, LUA_REGISTRYINDEX, luaL_array_metatable_ref);
	lua_setfield(L, -2, "array_mt");
	lua_rawgeti(L, LUA_REGISTRYINDEX, luaL_map_metatable_ref);
	lua_setfield(L, -2, "map_mt");

	if (modname != NULL) {
		/* Register module */
		lua_getfield(L, LUA_REGISTRYINDEX, "_LOADED");
		lua_pushstring(L, modname); /* add alias */
		lua_pushvalue(L, -3);
		lua_settable(L, -3);
		lua_pop(L, 1); /* _LOADED */
	}

	return serializer;
}

static int
lua_gettable_wrapper(lua_State *L)
{
	lua_gettable(L, -2);
	return 1;
}

static void
lua_field_inspect_ucdata(struct lua_State *L, struct luaL_serializer *cfg,
			int idx, struct luaL_field *field)
{
	if (!cfg->encode_load_metatables)
		return;

	/*
	 * Try to call LUAL_SERIALIZE method on udata/cdata
	 * LuaJIT specific: lua_getfield/lua_gettable raises exception on
	 * cdata if field doesn't exist.
	 */
	int top = lua_gettop(L);
	lua_pushcfunction(L, lua_gettable_wrapper);
	lua_pushvalue(L, idx);
	lua_pushliteral(L, LUAL_SERIALIZE);
	if (lua_pcall(L, 2, 1, 0) == 0  && !lua_isnil(L, -1)) {
		if (!lua_isfunction(L, -1))
			luaL_error(L, "invalid " LUAL_SERIALIZE  " value");
		/* copy object itself */
		lua_pushvalue(L, idx);
		lua_pcall(L, 1, 1, 0);
		/* replace obj with the unpacked value */
		lua_replace(L, idx);
		if (luaL_tofield(L, cfg, idx, field) < 0)
			luaT_error(L);
	} /* else ignore lua_gettable exceptions */
	lua_settop(L, top); /* remove temporary objects */
}

static int
lua_field_inspect_table(struct lua_State *L, struct luaL_serializer *cfg,
			int idx, struct luaL_field *field)
{
	assert(lua_type(L, idx) == LUA_TTABLE);
	const char *type;
	uint32_t size = 0;
	uint32_t max = 0;

	/* Try to get field LUAL_SERIALIZER_TYPE from metatable */
	if (!cfg->encode_load_metatables ||
	    !luaL_getmetafield(L, idx, LUAL_SERIALIZE))
		goto skip;

	if (lua_isfunction(L, -1)) {
		/* copy object itself */
		lua_pushvalue(L, idx);
		lua_call(L, 1, 1);
		/* replace obj with the unpacked value */
		lua_replace(L, idx);
		return luaL_tofield(L, cfg, idx, field);
	} else if (!lua_isstring(L, -1)) {
		diag_set(ClientError, ER_PROC_LUA,
			 "invalid " LUAL_SERIALIZE " value");
		return -1;
	}

	type = lua_tostring(L, -1);
	if (strcmp(type, "array") == 0 || strcmp(type, "seq") == 0 ||
	    strcmp(type, "sequence") == 0) {
		field->type = MP_ARRAY; /* Override type */
		field->size = luaL_arrlen(L, idx);
		/* YAML: use flow mode if __serialize == 'seq' */
		if (cfg->has_compact && type[3] == '\0')
			field->compact = true;
		lua_pop(L, 1); /* type */

		return 0;
	} else if (strcmp(type, "map") == 0 || strcmp(type, "mapping") == 0) {
		field->type = MP_MAP;   /* Override type */
		field->size = luaL_maplen(L, idx);
		/* YAML: use flow mode if __serialize == 'map' */
		if (cfg->has_compact && type[3] == '\0')
			field->compact = true;
		lua_pop(L, 1); /* type */
		return 0;
	} else {
		diag_set(ClientError, ER_PROC_LUA,
			 "invalid " LUAL_SERIALIZE " value");
		return -1;
	}

skip:
	field->type = MP_ARRAY;

	/* Calculate size and check that table can represent an array */
	lua_pushnil(L);
	while (lua_next(L, idx)) {
		size++;
		lua_pop(L, 1); /* pop the value */
		lua_Number k;
		if (lua_type(L, -1) != LUA_TNUMBER ||
		    ((k = lua_tonumber(L, -1)) != size &&
		     (k < 1 || floor(k) != k))) {
			/* Finish size calculation */
			while (lua_next(L, idx)) {
				size++;
				lua_pop(L, 1); /* pop the value */
			}
			field->type = MP_MAP;
			field->size = size;
			return 0;
		}
		if (k > max)
			max = k;
	}

	/* Encode excessively sparse arrays as objects (if enabled) */
	if (cfg->encode_sparse_ratio > 0 &&
	    max > size * (uint32_t)cfg->encode_sparse_ratio &&
	    max > (uint32_t)cfg->encode_sparse_safe) {
		if (!cfg->encode_sparse_convert) {
			diag_set(ClientError, ER_PROC_LUA,
				 "excessively sparse array");
			return -1;
		}
		field->type = MP_MAP;
		field->size = size;
		return 0;
	}

	assert(field->type == MP_ARRAY);
	field->size = max;
	return 0;
}

static void
lua_field_tostring(struct lua_State *L, struct luaL_serializer *cfg, int idx,
		   struct luaL_field *field)
{
	int top = lua_gettop(L);
	lua_getglobal(L, "tostring");
	lua_pushvalue(L, idx);
	lua_call(L, 1, 1);
	lua_replace(L, idx);
	lua_settop(L, top);
	if (luaL_tofield(L, cfg, idx, field) < 0)
		luaT_error(L);
}

int
luaL_tofield(struct lua_State *L, struct luaL_serializer *cfg, int index,
	     struct luaL_field *field)
{
	if (index < 0)
		index = lua_gettop(L) + index + 1;

	double num;
	double intpart;
	size_t size;

#define CHECK_NUMBER(x) ({							\
	if (!isfinite(x) && !cfg->encode_invalid_numbers) {			\
		if (!cfg->encode_invalid_as_nil) {				\
			diag_set(ClientError, ER_PROC_LUA,			\
				 "number must not be NaN or Inf");		\
			return -1;						\
		}								\
		field->type = MP_NIL;						\
	}})

	switch (lua_type(L, index)) {
	case LUA_TNUMBER:
		num = lua_tonumber(L, index);
		if (isfinite(num) && modf(num, &intpart) != 0.0) {
			field->type = MP_DOUBLE;
			field->dval = num;
		} else if (num >= 0 && num < exp2(64)) {
			field->type = MP_UINT;
			field->ival = (uint64_t) num;
		} else if (num > -exp2(63) && num < exp2(63)) {
			field->type = MP_INT;
			field->ival = (int64_t) num;
		} else {
			field->type = MP_DOUBLE;
			field->dval = num;
			CHECK_NUMBER(num);
		}
		break;
	case LUA_TCDATA:
	{
		GCcdata *cd = cdataV(L->base + index - 1);
		void *cdata = (void *)cdataptr(cd);

		int64_t ival;
		switch (cd->ctypeid) {
		case CTID_BOOL:
			field->type = MP_BOOL;
			field->bval = *(bool*) cdata;
			break;
		case CTID_CCHAR:
		case CTID_INT8:
			ival = *(int8_t *) cdata;
			field->type = (ival >= 0) ? MP_UINT : MP_INT;
			field->ival = ival;
			break;
		case CTID_INT16:
			ival = *(int16_t *) cdata;
			field->type = (ival >= 0) ? MP_UINT : MP_INT;
			field->ival = ival;
			break;
		case CTID_INT32:
			ival = *(int32_t *) cdata;
			field->type = (ival >= 0) ? MP_UINT : MP_INT;
			field->ival = ival;
			break;
		case CTID_INT64:
			ival = *(int64_t *) cdata;
			field->type = (ival >= 0) ? MP_UINT : MP_INT;
			field->ival = ival;
			break;
		case CTID_UINT8:
			field->type = MP_UINT;
			field->ival = *(uint8_t *) cdata;
			break;
		case CTID_UINT16:
			field->type = MP_UINT;
			field->ival = *(uint16_t *) cdata;
			break;
		case CTID_UINT32:
			field->type = MP_UINT;
			field->ival = *(uint32_t *) cdata;
			break;
		case CTID_UINT64:
			field->type = MP_UINT;
			field->ival = *(uint64_t *) cdata;
			break;
		case CTID_FLOAT:
			field->type = MP_FLOAT;
			field->fval = *(float *) cdata;
			CHECK_NUMBER(field->fval);
			break;
		case CTID_DOUBLE:
			field->type = MP_DOUBLE;
			field->dval = *(double *) cdata;
			CHECK_NUMBER(field->dval);
			break;
		case CTID_P_CVOID:
		case CTID_P_VOID:
			if (*(void **) cdata == NULL) {
				field->type = MP_NIL;
				break;
			}
			/* Fall through */
		default:
			field->type = MP_EXT;
			break;
		}
		break;
	}
	case LUA_TBOOLEAN:
		field->type = MP_BOOL;
		field->bval = lua_toboolean(L, index);
		break;
	case LUA_TNIL:
		field->type = MP_NIL;
		break;
	case LUA_TSTRING:
		field->sval.data = lua_tolstring(L, index, &size);
		field->sval.len = (uint32_t) size;
		field->type = MP_STR;
		break;
	case LUA_TTABLE:
	{
		field->compact = false;
		if (lua_field_inspect_table(L, cfg, index, field) < 0)
			return -1;
		break;
	}
	case LUA_TLIGHTUSERDATA:
	case LUA_TUSERDATA:
		field->sval.data = NULL;
		field->sval.len = 0;
		if (lua_touserdata(L, index) == NULL) {
			field->type = MP_NIL;
			break;
		}
		/* Fall through */
	default:
		field->type = MP_EXT;
		break;
	}
#undef CHECK_NUMBER
	return 0;
}

void
luaL_convertfield(struct lua_State *L, struct luaL_serializer *cfg, int idx,
		  struct luaL_field *field)
{
	if (idx < 0)
		idx = lua_gettop(L) + idx + 1;
	assert(field->type == MP_EXT); /* must be called after tofield() */

	if (cfg->encode_load_metatables) {
		int type = lua_type(L, idx);
		if (type == LUA_TCDATA) {
			/*
			 * Don't call __serialize on primitive types
			 * https://github.com/tarantool/tarantool/issues/1226
			 */
			GCcdata *cd = cdataV(L->base + idx - 1);
			if (cd->ctypeid > CTID_CTYPEID)
				lua_field_inspect_ucdata(L, cfg, idx, field);
		} else if (type == LUA_TUSERDATA) {
			lua_field_inspect_ucdata(L, cfg, idx, field);
		}
	}

	if (field->type == MP_EXT && cfg->encode_use_tostring)
		lua_field_tostring(L, cfg, idx, field);

	if (field->type != MP_EXT)
		return;

	if (cfg->encode_invalid_as_nil) {
		field->type = MP_NIL;
		return;
	}

	luaL_error(L, "unsupported Lua type '%s'",
		   lua_typename(L, lua_type(L, idx)));
}

/**
 * A helper to register a single type metatable.
 */
void
luaL_register_type(struct lua_State *L, const char *type_name,
		   const struct luaL_Reg *methods)
{
	luaL_newmetatable(L, type_name);
	/*
	 * Conventionally, make the metatable point to itself
	 * in __index. If 'methods' contain a field for __index,
	 * this is a no-op.
	 */
	lua_pushvalue(L, -1);
	lua_setfield(L, -2, "__index");
	lua_pushstring(L, type_name);
	lua_setfield(L, -2, "__metatable");
	luaL_register(L, NULL, methods);
	lua_pop(L, 1);
}

void
luaL_register_module(struct lua_State *L, const char *modname,
		     const struct luaL_Reg *methods)
{
	assert(methods != NULL && modname != NULL); /* use luaL_register instead */
	lua_getfield(L, LUA_REGISTRYINDEX, "_LOADED");
	if (strchr(modname, '.') == NULL) {
		/* root level, e.g. box */
		lua_getfield(L, -1, modname); /* get package.loaded.modname */
		if (!lua_istable(L, -1)) {  /* module is not found */
			lua_pop(L, 1);  /* remove previous result */
			lua_newtable(L);
			lua_pushvalue(L, -1);
			lua_setfield(L, -3, modname);  /* _LOADED[modname] = new table */
		}
	} else {
		/* 1+ level, e.g. box.space */
		if (luaL_findtable(L, -1, modname, 0) != NULL)
			luaL_error(L, "Failed to register library");
	}
	lua_remove(L, -2);  /* remove _LOADED table */
	luaL_register(L, NULL, methods);
}

/*
 * Maximum integer that doesn't lose precision on tostring() conversion.
 * Lua uses sprintf("%.14g") to format its numbers, see gh-1279.
 */
#define DBL_INT_MAX (1e14 - 1)
#define DBL_INT_MIN (-1e14 + 1)

void
luaL_pushuint64(struct lua_State *L, uint64_t val)
{
#if defined(LJ_DUALNUM) /* see setint64V() */
	if (val <= INT32_MAX) {
		/* push int32_t */
		lua_pushinteger(L, (int32_t) val);
	} else
#endif /* defined(LJ_DUALNUM) */
	if (val <= DBL_INT_MAX) {
		/* push double */
		lua_pushnumber(L, (double) val);
	} else {
		/* push uint64_t */
		*(uint64_t *) luaL_pushcdata(L, CTID_UINT64) = val;
	}
}

void
luaL_pushint64(struct lua_State *L, int64_t val)
{
#if defined(LJ_DUALNUM) /* see setint64V() */
	if (val >= INT32_MIN && val <= INT32_MAX) {
		/* push int32_t */
		lua_pushinteger(L, (int32_t) val);
	} else
#endif /* defined(LJ_DUALNUM) */
	if (val >= DBL_INT_MIN && val <= DBL_INT_MAX) {
		/* push double */
		lua_pushnumber(L, (double) val);
	} else {
		/* push int64_t */
		*(int64_t *) luaL_pushcdata(L, CTID_INT64) = val;
	}
}

static inline int
luaL_convertint64(lua_State *L, int idx, bool unsignd, int64_t *result)
{
	uint32_t ctypeid;
	void *cdata;
	/*
	 * This code looks mostly like luaL_tofield(), but has less
	 * cases and optimized for numbers.
	 */
	switch (lua_type(L, idx)) {
	case LUA_TNUMBER:
		*result = lua_tonumber(L, idx);
		return 0;
	case LUA_TCDATA:
		cdata = luaL_checkcdata(L, idx, &ctypeid);
		switch (ctypeid) {
		case CTID_CCHAR:
		case CTID_INT8:
			*result = *(int8_t *) cdata;
			return 0;
		case CTID_INT16:
			*result = *(int16_t *) cdata;
			return 0;
		case CTID_INT32:
			*result = *(int32_t *) cdata;
			return 0;
		case CTID_INT64:
			*result = *(int64_t *) cdata;
			return 0;
		case CTID_UINT8:
			*result = *(uint8_t *) cdata;
			return 0;
		case CTID_UINT16:
			*result = *(uint16_t *) cdata;
			return 0;
		case CTID_UINT32:
			*result = *(uint32_t *) cdata;
			return 0;
		case CTID_UINT64:
			*result = *(uint64_t *) cdata;
			return 0;
		}
		*result = 0;
		return -1;
	case LUA_TSTRING:
	{
		const char *arg = luaL_checkstring(L, idx);
		char *arge;
		errno = 0;
		*result = (unsignd ? (long long) strtoull(arg, &arge, 10) :
			strtoll(arg, &arge, 10));
		if (errno == 0 && arge != arg)
			return 0;
		return 1;
	}
	}
	*result = 0;
	return -1;
}

uint64_t
luaL_checkuint64(struct lua_State *L, int idx)
{
	int64_t result;
	if (luaL_convertint64(L, idx, true, &result) != 0) {
		lua_pushfstring(L, "expected uint64_t as %d argument", idx);
		lua_error(L);
		return 0;
	}
	return result;
}

int64_t
luaL_checkint64(struct lua_State *L, int idx)
{
	int64_t result;
	if (luaL_convertint64(L, idx, false, &result) != 0) {
		lua_pushfstring(L, "expected int64_t as %d argument", idx);
		lua_error(L);
		return 0;
	}
	return result;
}

uint64_t
luaL_touint64(struct lua_State *L, int idx)
{
	int64_t result;
	if (luaL_convertint64(L, idx, true, &result) == 0)
		return result;
	return 0;
}

int64_t
luaL_toint64(struct lua_State *L, int idx)
{
	int64_t result;
	if (luaL_convertint64(L, idx, false, &result) == 0)
		return result;
	return 0;
}


int
luaT_toerror(lua_State *L)
{
	struct error *e = luaL_iserror(L, -1);
	if (e != NULL) {
		/* Re-throw original error */
		diag_add_error(&fiber()->diag, e);
	} else {
		/* Convert Lua error to a Tarantool exception. */
		diag_set(LuajitError, luaT_tolstring(L, -1, NULL));
	}
	return 1;
}

int
luaT_call(struct lua_State *L, int nargs, int nreturns)
{
	if (lua_pcall(L, nargs, nreturns, 0))
		return luaT_toerror(L);
	return 0;
}

int
luaT_cpcall(lua_State *L, lua_CFunction func, void *ud)
{
	if (lua_cpcall(L, func, ud))
		return luaT_toerror(L);
	return 0;
}

/**
 * This function exists because lua_tostring does not use
 * __tostring metamethod, and this metamethod has to be used
 * if we want to print Lua userdata correctly.
 */
const char *
luaT_tolstring(lua_State *L, int idx, size_t *len)
{
	if (!luaL_callmeta(L, idx, "__tostring")) {
		switch (lua_type(L, idx)) {
		case LUA_TNUMBER:
		case LUA_TSTRING:
			lua_pushvalue(L, idx);
			break;
		case LUA_TBOOLEAN: {
			int val = lua_toboolean(L, idx);
			lua_pushstring(L, val ? "true" : "false");
			break;
		}
		case LUA_TNIL:
			lua_pushliteral(L, "nil");
			break;
		default:
			lua_pushfstring(L, "%s: %p", luaL_typename(L, idx),
						     lua_topointer(L, idx));
		}
	}

	return lua_tolstring(L, -1, len);
}

lua_State *
luaT_state(void)
{
	return tarantool_L;
}

int
tarantool_lua_utils_init(struct lua_State *L)
{
	static const struct luaL_Reg serializermeta[] = {
		{NULL, NULL},
	};

	luaL_register_type(L, LUAL_SERIALIZER, serializermeta);
	/* Create NULL constant */
	*(void **) luaL_pushcdata(L, CTID_P_VOID) = NULL;
	luaL_nil_ref = luaL_ref(L, LUA_REGISTRYINDEX);

	lua_createtable(L, 0, 1);
	lua_pushliteral(L, "map"); /* YAML will use flow mode */
	lua_setfield(L, -2, LUAL_SERIALIZE);
	/* automatically reset hints on table change */
	luaL_loadstring(L, "setmetatable((...), nil); return rawset(...)");
	lua_setfield(L, -2, "__newindex");
	luaL_map_metatable_ref = luaL_ref(L, LUA_REGISTRYINDEX);

	lua_createtable(L, 0, 1);
	lua_pushliteral(L, "seq"); /* YAML will use flow mode */
	lua_setfield(L, -2, LUAL_SERIALIZE);
	/* automatically reset hints on table change */
	luaL_loadstring(L, "setmetatable((...), nil); return rawset(...)");
	lua_setfield(L, -2, "__newindex");
	luaL_array_metatable_ref = luaL_ref(L, LUA_REGISTRYINDEX);

	return 0;
}
