#include <string.h>           /* strncmp() */
#include <lua.h>              /* lua_*() */
#include <lauxlib.h>          /* luaL_*() */
#include <lualib.h>           /* luaL_openlibs() */
#include "unit.h"             /* plan, header, footer, is, ok */
#include "memory.h"           /* memory_init() */
#include "fiber.h"            /* fiber_init() */
#include "small/ibuf.h"       /* struct ibuf */
#include "box/box.h"          /* box_init() */
#include "box/tuple.h"        /* box_tuple_format_default() */
#include "lua/msgpack.h"      /* luaopen_msgpack() */
#include "box/lua/tuple.h"    /* luaL_iterator_*() */

/*
 * This test checks all usage cases of luaT_tuple_new():
 *
 * * Use with idx == 0 and idx != 0.
 * * Use with default and non-default formats.
 * * Use a table and a tuple as an input.
 * * Use with an unexpected lua type as an input.
 *
 * The test does not vary an input table/tuple. This is done in
 * box/tuple.test.lua.
 */

extern struct ibuf *tarantool_lua_ibuf;

uint32_t
min_u32(uint32_t a, uint32_t b)
{
	return a < b ? a : b;
}

void
check_tuple(const struct tuple *tuple, box_tuple_format_t *format,
	    int retvals, const char *case_name)
{
	uint32_t size;
	const char *data = tuple_data_range(tuple, &size);

	ok(tuple != NULL, "%s: tuple != NULL", case_name);
	is(tuple->format_id, tuple_format_id(format),
	   "%s: check tuple format id", case_name);
	is(size, 4, "%s: check tuple size", case_name);
	ok(!strncmp(data, "\x93\x01\x02\x03", min_u32(size, 4)),
	   "%s: check tuple data", case_name);
	is(retvals, 0, "%s: check retvals count", case_name);
}

void check_error(struct lua_State *L, const struct tuple *tuple, int retvals,
		 const char *case_name)
{
	const char *exp_err = "A tuple or a table expected, got number";
	is(tuple, NULL, "%s: tuple == NULL", case_name);
	is(retvals, 1, "%s: check retvals count", case_name);
	is(lua_type(L, -1), LUA_TSTRING, "%s: check error type", case_name);
	ok(!strcmp(lua_tostring(L, -1), exp_err), "%s: check error message",
	   case_name);
}

int
test_basic(struct lua_State *L)
{
	plan(19);
	header();

	int top;
	struct tuple *tuple;
	box_tuple_format_t *default_format = box_tuple_format_default();

	/*
	 * Case: a Lua table on idx == -2 as an input.
	 */

	/* Prepare the Lua stack. */
	luaL_loadstring(L, "return {1, 2, 3}");
	lua_call(L, 0, 1);
	lua_pushnil(L);

	/* Create and check a tuple. */
	top = lua_gettop(L);
	tuple = luaT_tuple_new(L, -2, default_format);
	check_tuple(tuple, default_format, lua_gettop(L) - top, "table");

	/* Clean up. */
	lua_pop(L, 2);
	assert(lua_gettop(L) == 0);

	/*
	 * Case: a tuple on idx == -1 as an input.
	 */

	/* Prepare the Lua stack. */
	luaT_pushtuple(L, tuple);

	/* Create and check a tuple. */
	top = lua_gettop(L);
	tuple = luaT_tuple_new(L, -1, default_format);
	check_tuple(tuple, default_format, lua_gettop(L) - top, "tuple");

	/* Clean up. */
	lua_pop(L, 1);
	assert(lua_gettop(L) == 0);

	/*
	 * Case: elements on the stack (idx == 0) as an input and
	 * a non-default format.
	 */

	/* Prepare the Lua stack. */
	lua_pushinteger(L, 1);
	lua_pushinteger(L, 2);
	lua_pushinteger(L, 3);

	/* Create a new format. */
	struct key_part_def part;
	part.fieldno = 0;
	part.type = FIELD_TYPE_INTEGER;
	part.coll_id = COLL_NONE;
	part.is_nullable = false;
	part.nullable_action = ON_CONFLICT_ACTION_DEFAULT;
	part.sort_order = SORT_ORDER_ASC;
	struct key_def *key_def = key_def_new(&part, 1);
	box_tuple_format_t *another_format = box_tuple_format_new(&key_def, 1);
	key_def_delete(key_def);

	/* Create and check a tuple. */
	top = lua_gettop(L);
	tuple = luaT_tuple_new(L, 0, another_format);
	check_tuple(tuple, another_format, lua_gettop(L) - top, "objects");

	/* Clean up. */
	tuple_format_delete(another_format);
	lua_pop(L, 3);
	assert(lua_gettop(L) == 0);

	/*
	 * Case: a lua object of an unexpected type.
	 */

	/* Prepare the Lua stack. */
	lua_pushinteger(L, 42);

	/* Try to create and check for the error. */
	top = lua_gettop(L);
	tuple = luaT_tuple_new(L, -1, default_format);
	check_error(L, tuple, lua_gettop(L) - top, "unexpected type");

	/* Clean up. */
	lua_pop(L, 2);
	assert(lua_gettop(L) == 0);

	footer();
	return check_plan();
}

int
main()
{
	memory_init();
	fiber_init(fiber_c_invoke);

	ibuf_create(tarantool_lua_ibuf, &cord()->slabc, 16000);

	struct lua_State *L = luaL_newstate();
	luaL_openlibs(L);

	box_init();
	box_lua_tuple_init(L);
	luaopen_msgpack(L);
	lua_pop(L, 1);

	return test_basic(L);
}
