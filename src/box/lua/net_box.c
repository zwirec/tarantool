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
#include "net_box.h"
#include <sys/socket.h>

#include <small/ibuf.h>
#include <msgpuck.h> /* mp_store_u32() */
#include "scramble.h"

#include "box/iproto_constants.h"
#include "box/lua/tuple.h" /* luamp_convert_tuple() / luamp_convert_key() */
#include "box/xrow.h"
#include "box/tuple.h"
#include "box/execute.h"

#include "lua/msgpack.h"
#include "third_party/base64.h"

#include "coio.h"
#include "box/errcode.h"
#include "lua/fiber.h"
#include "mpstream.h"

#define cfg luaL_msgpack_default

static uint32_t CTID_CHAR_PTR;
static uint32_t CTID_CONST_CHAR_PTR;

static inline size_t
netbox_prepare_request(lua_State *L, struct mpstream *stream, uint32_t r_type)
{
	struct ibuf *ibuf = (struct ibuf *) lua_topointer(L, 1);
	uint64_t sync = luaL_touint64(L, 2);

	mpstream_init(stream, ibuf, ibuf_reserve_cb, ibuf_alloc_cb,
		      luamp_error, L);

	/* Remember initial size of ibuf (see netbox_encode_request()) */
	size_t used = ibuf_used(ibuf);

	/* Reserve and skip space for fixheader */
	size_t fixheader_size = mp_sizeof_uint(UINT32_MAX);
	mpstream_reserve(stream, fixheader_size);
	mpstream_advance(stream, fixheader_size);

	/* encode header */
	mpstream_encode_map(stream, 2);

	mpstream_encode_uint(stream, IPROTO_SYNC);
	mpstream_encode_uint(stream, sync);

	mpstream_encode_uint(stream, IPROTO_REQUEST_TYPE);
	mpstream_encode_uint(stream, r_type);

	/* Caller should remember how many bytes was used in ibuf */
	return used;
}

static inline void
netbox_encode_request(struct mpstream *stream, size_t initial_size)
{
	mpstream_flush(stream);

	struct ibuf *ibuf = (struct ibuf *) stream->ctx;

	/*
	 * Calculation the start position in ibuf by getting current size
	 * and then substracting initial size. Since we don't touch
	 * ibuf->rpos during encoding this approach should always work
	 * even on realloc or memmove inside ibuf.
	 */
	size_t fixheader_size = mp_sizeof_uint(UINT32_MAX);
	size_t used = ibuf_used(ibuf);
	assert(initial_size + fixheader_size <= used);
	size_t total_size = used - initial_size;
	char *fixheader = ibuf->wpos - total_size;
	assert(fixheader >= ibuf->rpos);

	/* patch skipped len */
	*(fixheader++) = 0xce;
	/* fixheader size is not included */
	mp_store_u32(fixheader, total_size - fixheader_size);
}

static int
netbox_encode_ping(lua_State *L)
{
	if (lua_gettop(L) < 2)
		return luaL_error(L, "Usage: netbox.encode_ping(ibuf, sync)");

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_PING);
	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_auth(lua_State *L)
{
	if (lua_gettop(L) < 5) {
		return luaL_error(L, "Usage: netbox.encode_update(ibuf, sync, "
				     "user, password, greeting)");
	}

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_AUTH);

	size_t user_len;
	const char *user = lua_tolstring(L, 3, &user_len);
	size_t password_len;
	const char *password = lua_tolstring(L, 4, &password_len);
	size_t salt_len;
	const char *salt = lua_tolstring(L, 5, &salt_len);
	if (salt_len < SCRAMBLE_SIZE)
		return luaL_error(L, "Invalid salt");

	/* Adapted from xrow_encode_auth() */
	mpstream_encode_map(&stream, password != NULL ? 2 : 1);
	mpstream_encode_uint(&stream, IPROTO_USER_NAME);
	mpstream_encode_strn(&stream, user, user_len);
	if (password != NULL) { /* password can be omitted */
		char scramble[SCRAMBLE_SIZE];
		scramble_prepare(scramble, salt, password, password_len);
		mpstream_encode_uint(&stream, IPROTO_TUPLE);
		mpstream_encode_array(&stream, 2);
		mpstream_encode_str(&stream, "chap-sha1");
		mpstream_encode_strn(&stream, scramble, SCRAMBLE_SIZE);
	}

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_call_impl(lua_State *L, enum iproto_type type)
{
	if (lua_gettop(L) < 4) {
		return luaL_error(L, "Usage: netbox.encode_call(ibuf, sync, "
				     "function_name, args)");
	}

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, type);

	mpstream_encode_map(&stream, 2);

	/* encode proc name */
	size_t name_len;
	const char *name = lua_tolstring(L, 3, &name_len);
	mpstream_encode_uint(&stream, IPROTO_FUNCTION_NAME);
	mpstream_encode_strn(&stream, name, name_len);

	/* encode args */
	mpstream_encode_uint(&stream, IPROTO_TUPLE);
	luamp_encode_tuple(L, cfg, &stream, 4);

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_call_16(lua_State *L)
{
	return netbox_encode_call_impl(L, IPROTO_CALL_16);
}

static int
netbox_encode_call(lua_State *L)
{
	return netbox_encode_call_impl(L, IPROTO_CALL);
}

static int
netbox_encode_eval(lua_State *L)
{
	if (lua_gettop(L) < 4) {
		return luaL_error(L, "Usage: netbox.encode_eval(ibuf, sync, "
				     "expr, args)");
	}

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_EVAL);

	mpstream_encode_map(&stream, 2);

	/* encode expr */
	size_t expr_len;
	const char *expr = lua_tolstring(L, 3, &expr_len);
	mpstream_encode_uint(&stream, IPROTO_EXPR);
	mpstream_encode_strn(&stream, expr, expr_len);

	/* encode args */
	mpstream_encode_uint(&stream, IPROTO_TUPLE);
	luamp_encode_tuple(L, cfg, &stream, 4);

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_select(lua_State *L)
{
	if (lua_gettop(L) < 8) {
		return luaL_error(L, "Usage netbox.encode_select(ibuf, sync, "
				     "space_id, index_id, iterator, offset, "
				     "limit, key)");
	}

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_SELECT);

	mpstream_encode_map(&stream, 6);

	uint32_t space_id = lua_tonumber(L, 3);
	uint32_t index_id = lua_tonumber(L, 4);
	int iterator = lua_tointeger(L, 5);
	uint32_t offset = lua_tonumber(L, 6);
	uint32_t limit = lua_tonumber(L, 7);

	/* encode space_id */
	mpstream_encode_uint(&stream, IPROTO_SPACE_ID);
	mpstream_encode_uint(&stream, space_id);

	/* encode index_id */
	mpstream_encode_uint(&stream, IPROTO_INDEX_ID);
	mpstream_encode_uint(&stream, index_id);

	/* encode iterator */
	mpstream_encode_uint(&stream, IPROTO_ITERATOR);
	mpstream_encode_uint(&stream, iterator);

	/* encode offset */
	mpstream_encode_uint(&stream, IPROTO_OFFSET);
	mpstream_encode_uint(&stream, offset);

	/* encode limit */
	mpstream_encode_uint(&stream, IPROTO_LIMIT);
	mpstream_encode_uint(&stream, limit);

	/* encode key */
	mpstream_encode_uint(&stream, IPROTO_KEY);
	luamp_convert_key(L, cfg, &stream, 8);

	netbox_encode_request(&stream, svp);
	return 0;
}

static inline int
netbox_encode_insert_or_replace(lua_State *L, uint32_t reqtype)
{
	if (lua_gettop(L) < 4) {
		return luaL_error(L, "Usage: netbox.encode_insert(ibuf, sync, "
				     "space_id, tuple)");
	}
	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, reqtype);

	mpstream_encode_map(&stream, 2);

	/* encode space_id */
	uint32_t space_id = lua_tonumber(L, 3);
	mpstream_encode_uint(&stream, IPROTO_SPACE_ID);
	mpstream_encode_uint(&stream, space_id);

	/* encode args */
	mpstream_encode_uint(&stream, IPROTO_TUPLE);
	luamp_encode_tuple(L, cfg, &stream, 4);

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_insert(lua_State *L)
{
	return netbox_encode_insert_or_replace(L, IPROTO_INSERT);
}

static int
netbox_encode_replace(lua_State *L)
{
	return netbox_encode_insert_or_replace(L, IPROTO_REPLACE);
}

static int
netbox_encode_delete(lua_State *L)
{
	if (lua_gettop(L) < 5) {
		return luaL_error(L, "Usage: netbox.encode_delete(ibuf, sync, "
				     "space_id, index_id, key)");
	}

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_DELETE);

	mpstream_encode_map(&stream, 3);

	/* encode space_id */
	uint32_t space_id = lua_tonumber(L, 3);
	mpstream_encode_uint(&stream, IPROTO_SPACE_ID);
	mpstream_encode_uint(&stream, space_id);

	/* encode space_id */
	uint32_t index_id = lua_tonumber(L, 4);
	mpstream_encode_uint(&stream, IPROTO_INDEX_ID);
	mpstream_encode_uint(&stream, index_id);

	/* encode key */
	mpstream_encode_uint(&stream, IPROTO_KEY);
	luamp_convert_key(L, cfg, &stream, 5);

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_update(lua_State *L)
{
	if (lua_gettop(L) < 6) {
		return luaL_error(L, "Usage: netbox.encode_update(ibuf, sync, "
				     "space_id, index_id, key, ops)");
	}

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_UPDATE);

	mpstream_encode_map(&stream, 5);

	/* encode space_id */
	uint32_t space_id = lua_tonumber(L, 3);
	mpstream_encode_uint(&stream, IPROTO_SPACE_ID);
	mpstream_encode_uint(&stream, space_id);

	/* encode index_id */
	uint32_t index_id = lua_tonumber(L, 4);
	mpstream_encode_uint(&stream, IPROTO_INDEX_ID);
	mpstream_encode_uint(&stream, index_id);

	/* encode index_id */
	mpstream_encode_uint(&stream, IPROTO_INDEX_BASE);
	mpstream_encode_uint(&stream, 1);

	/* encode in reverse order for speedup - see luamp_encode() code */
	/* encode ops */
	mpstream_encode_uint(&stream, IPROTO_TUPLE);
	luamp_encode_tuple(L, cfg, &stream, 6);
	lua_pop(L, 1); /* ops */

	/* encode key */
	mpstream_encode_uint(&stream, IPROTO_KEY);
	luamp_convert_key(L, cfg, &stream, 5);

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_encode_upsert(lua_State *L)
{
	if (lua_gettop(L) != 5) {
		return luaL_error(L, "Usage: netbox.encode_upsert(ibuf, sync, "
				     "space_id, tuple, ops)");
	}

	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_UPSERT);

	mpstream_encode_map(&stream, 4);

	/* encode space_id */
	uint32_t space_id = lua_tonumber(L, 3);
	mpstream_encode_uint(&stream, IPROTO_SPACE_ID);
	mpstream_encode_uint(&stream, space_id);

	/* encode index_base */
	mpstream_encode_uint(&stream, IPROTO_INDEX_BASE);
	mpstream_encode_uint(&stream, 1);

	/* encode in reverse order for speedup - see luamp_encode() code */
	/* encode ops */
	mpstream_encode_uint(&stream, IPROTO_OPS);
	luamp_encode_tuple(L, cfg, &stream, 5);
	lua_pop(L, 1); /* ops */

	/* encode tuple */
	mpstream_encode_uint(&stream, IPROTO_TUPLE);
	luamp_encode_tuple(L, cfg, &stream, 4);

	netbox_encode_request(&stream, svp);
	return 0;
}

static int
netbox_decode_greeting(lua_State *L)
{
	struct greeting greeting;
	size_t len;
	const char *buf = NULL;
	char uuid_buf[UUID_STR_LEN + 1];

	if (lua_isstring(L, 1))
		buf = lua_tolstring(L, 1, &len);

	if (buf == NULL || len != IPROTO_GREETING_SIZE ||
		greeting_decode(buf, &greeting) != 0) {

		lua_pushboolean(L, 0);
		lua_pushstring(L, "Invalid greeting");
		return 2;
	}

	lua_newtable(L);
	lua_pushinteger(L, greeting.version_id);
	lua_setfield(L, -2, "version_id");
	lua_pushstring(L, greeting.protocol);
	lua_setfield(L, -2, "protocol");
	lua_pushlstring(L, greeting.salt, greeting.salt_len);
	lua_setfield(L, -2, "salt");

	tt_uuid_to_string(&greeting.uuid, uuid_buf);
	lua_pushstring(L, uuid_buf);
	lua_setfield(L, -2, "uuid");

	return 1;
}

/**
 * communicate(fd, send_buf, recv_buf, limit_or_boundary, timeout)
 *  -> errno, error
 *  -> nil, limit/boundary_pos
 *
 * The need for this function arises from not wanting to
 * have more than one watcher for a single fd, and thus issue
 * redundant epoll_ctl(EPOLLCTL_ADD) for it when doing both
 * reading and writing.
 *
 * Instead, this function takes an fd, input and output buffer,
 * and does sending and receiving on it in a single event loop
 * interaction.
 */
static int
netbox_communicate(lua_State *L)
{
	uint32_t fd = lua_tonumber(L, 1);
	const int NETBOX_READAHEAD = 16320;
	struct ibuf *send_buf = (struct ibuf *) lua_topointer(L, 2);
	struct ibuf *recv_buf = (struct ibuf *) lua_topointer(L, 3);

	/* limit or boundary */
	size_t limit = SIZE_MAX;
	const void *boundary = NULL;
	size_t boundary_len;

	if (lua_type(L, 4) == LUA_TSTRING)
		boundary = lua_tolstring(L, 4, &boundary_len);
	else
		limit = lua_tonumber(L, 4);

	/* timeout */
	ev_tstamp timeout = TIMEOUT_INFINITY;
	if (lua_type(L, 5) == LUA_TNUMBER)
		timeout = lua_tonumber(L, 5);
	if (timeout < 0) {
		lua_pushinteger(L, ER_TIMEOUT);
		lua_pushstring(L, "Timeout exceeded");
		return 2;
	}
	int revents = COIO_READ;
	while (true) {
		/* reader serviced first */
check_limit:
		if (ibuf_used(recv_buf) >= limit) {
			lua_pushnil(L);
			lua_pushinteger(L, (lua_Integer)limit);
			return 2;
		}
		const char *p;
		if (boundary != NULL && (p = memmem(
					recv_buf->rpos,
					ibuf_used(recv_buf),
					boundary, boundary_len)) != NULL) {
			lua_pushnil(L);
			lua_pushinteger(L, (lua_Integer)(
					p - recv_buf->rpos));
			return 2;
		}

		while (revents & COIO_READ) {
			void *p = ibuf_reserve(recv_buf, NETBOX_READAHEAD);
			if (p == NULL)
				luaL_error(L, "out of memory");
			ssize_t rc = recv(
				fd, recv_buf->wpos, ibuf_unused(recv_buf), 0);
			if (rc == 0) {
				lua_pushinteger(L, ER_NO_CONNECTION);
				lua_pushstring(L, "Peer closed");
				return 2;
			} if (rc > 0) {
				recv_buf->wpos += rc;
				goto check_limit;
			} else if (errno == EAGAIN || errno == EWOULDBLOCK)
				revents &= ~COIO_READ;
			else if (errno != EINTR)
				goto handle_error;
		}

		while ((revents & COIO_WRITE) && ibuf_used(send_buf) != 0) {
			ssize_t rc = send(
				fd, send_buf->rpos, ibuf_used(send_buf), 0);
			if (rc >= 0)
				send_buf->rpos += rc;
			else if (errno == EAGAIN || errno == EWOULDBLOCK)
				revents &= ~COIO_WRITE;
			else if (errno != EINTR)
				goto handle_error;
		}

		ev_tstamp deadline = ev_monotonic_now(loop()) + timeout;
		revents = coio_wait(fd, EV_READ | (ibuf_used(send_buf) != 0 ?
				EV_WRITE : 0), timeout);
		luaL_testcancel(L);
		timeout = deadline - ev_monotonic_now(loop());
		timeout = MAX(0.0, timeout);
		if (revents == 0 && timeout == 0.0) {
			lua_pushinteger(L, ER_TIMEOUT);
			lua_pushstring(L, "Timeout exceeded");
			return 2;
		}
	}
handle_error:
	lua_pushinteger(L, ER_NO_CONNECTION);
	lua_pushstring(L, strerror(errno));
	return 2;
}

static int
netbox_encode_execute(lua_State *L)
{
	if (lua_gettop(L) < 5)
		return luaL_error(L, "Usage: netbox.encode_execute(ibuf, "\
				  "sync, query, parameters, options)");
	struct mpstream stream;
	size_t svp = netbox_prepare_request(L, &stream, IPROTO_EXECUTE);

	mpstream_encode_map(&stream, 3);

	size_t len;
	const char *query = lua_tolstring(L, 3, &len);
	mpstream_encode_uint(&stream, IPROTO_SQL_TEXT);
	mpstream_encode_strn(&stream, query, len);

	mpstream_encode_uint(&stream, IPROTO_SQL_BIND);
	luamp_encode_tuple(L, cfg, &stream, 4);

	mpstream_encode_uint(&stream, IPROTO_OPTIONS);
	luamp_encode_tuple(L, cfg, &stream, 5);

	netbox_encode_request(&stream, svp);
	return 0;
}

/**
 * Decode IPROTO_DATA into tuples array.
 * @param L Lua stack to push result on.
 * @param data MessagePack.
 */
static void
netbox_decode_data(struct lua_State *L, const char **data)
{
	uint32_t count = mp_decode_array(data);
	lua_createtable(L, count, 0);
	struct tuple_format *format =
		box_tuple_format_default();
	for (uint32_t j = 0; j < count; ++j) {
		const char *begin = *data;
		mp_next(data);
		struct tuple *tuple =
			box_tuple_new(format, begin, *data);
		if (tuple == NULL)
			luaT_error(L);
		luaT_pushtuple(L, tuple);
		lua_rawseti(L, -2, j + 1);
	}
}

/**
 * Decode Tarantool response body consisting of single
 * IPROTO_DATA key into array of tuples.
 * @param Lua stack[1] Raw MessagePack pointer.
 * @retval Tuples array and position of the body end.
 */
static int
netbox_decode_select(struct lua_State *L)
{
	uint32_t ctypeid;
	const char *data = *(const char **)luaL_checkcdata(L, 1, &ctypeid);
	assert(mp_typeof(*data) == MP_MAP);
	uint32_t map_size = mp_decode_map(&data);
	/* Until 2.0 body has no keys except DATA. */
	assert(map_size == 1);
	(void) map_size;
	uint32_t key = mp_decode_uint(&data);
	assert(key == IPROTO_DATA);
	(void) key;
	netbox_decode_data(L, &data);
	*(const char **)luaL_pushcdata(L, ctypeid) = data;
	return 2;
}

/**
 * Decode IPROTO_METADATA into array of maps.
 * @param L Lua stack to push result on.
 * @param data MessagePack.
 */
static void
netbox_decode_metadata(struct lua_State *L, const char **data)
{
	uint32_t count = mp_decode_array(data);
	lua_createtable(L, count, 0);
	for (uint32_t i = 0; i < count; ++i) {
		uint32_t map_size = mp_decode_map(data);
		assert(map_size == 2);
		(void) map_size;
		uint32_t key = mp_decode_uint(data);
		assert(key == IPROTO_FIELD_NAME);
		(void) key;
		lua_createtable(L, 0, 1);
		uint32_t len;
		const char *str = mp_decode_str(data, &len);
		lua_pushlstring(L, str, len);
		lua_setfield(L, -2, "name");
		key = mp_decode_uint(data);
		assert(key == IPROTO_FIELD_TYPE);
		const char *type = mp_decode_str(data, &len);
		lua_pushlstring(L, type, len);
		lua_setfield(L, -2, "type");
		lua_rawseti(L, -2, i + 1);
	}
}

/**
 * Decode IPROTO_SQL_INFO into map.
 * @param L Lua stack to push result on.
 * @param data MessagePack.
 */
static void
netbox_decode_sql_info(struct lua_State *L, const char **data)
{
	uint32_t map_size = mp_decode_map(data);
	assert(map_size == 1 || map_size == 2);
	lua_newtable(L);
	/*
	 * First element in data is SQL_INFO_ROW_COUNT.
	 */
	uint32_t key = mp_decode_uint(data);
	assert(key == SQL_INFO_ROW_COUNT);
	uint32_t row_count = mp_decode_uint(data);
	lua_pushinteger(L, row_count);
	lua_setfield(L, -2, "rowcount");
	/*
	 * If data have two elements then second is
	 * SQL_INFO_AUTOINCREMENT_IDS.
	 */
	if (map_size == 2) {
		key = mp_decode_uint(data);
		assert(key == SQL_INFO_AUTOINCREMENT_IDS);
		(void)key;
		uint64_t count = mp_decode_array(data);
		assert(count > 0);
		lua_createtable(L, 0, count);
		for (uint32_t j = 0; j < count; ++j) {
			int64_t id;
			mp_read_int64(data, &id);
			luaL_pushint64(L, id);
			lua_rawseti(L, -2, j + 1);
		}
		lua_setfield(L, -2, "autoincrement_ids");
	}
}

static int
netbox_decode_execute(struct lua_State *L)
{
	uint32_t ctypeid;
	const char *data = *(const char **)luaL_checkcdata(L, 1, &ctypeid);
	assert(mp_typeof(*data) == MP_MAP);
	uint32_t map_size = mp_decode_map(&data);
	int rows_index = 0, meta_index = 0, info_index = 0;
	for (uint32_t i = 0; i < map_size; ++i) {
		uint32_t key = mp_decode_uint(&data);
		switch(key) {
		case IPROTO_DATA:
			netbox_decode_data(L, &data);
			rows_index = i - map_size;
			break;
		case IPROTO_METADATA:
			netbox_decode_metadata(L, &data);
			meta_index = i - map_size;
			break;
		default:
			assert(key == IPROTO_SQL_INFO);
			netbox_decode_sql_info(L, &data);
			info_index = i - map_size;
			break;
		}
	}
	if (info_index == 0) {
		assert(meta_index != 0);
		assert(rows_index != 0);
		lua_createtable(L, 0, 2);
		lua_pushvalue(L, meta_index - 1);
		lua_setfield(L, -2, "metadata");
		lua_pushvalue(L, rows_index - 1);
		lua_setfield(L, -2, "rows");
	} else {
		assert(meta_index == 0);
		assert(rows_index == 0);
	}
	*(const char **)luaL_pushcdata(L, ctypeid) = data;
	return 2;
}

/**
 * net_box.check_iproto_data(buf.rpos, buf.wpos - buf.rpos)
 *     -> new_rpos
 *     -> nil, err_msg
 */
int
netbox_check_iproto_data(struct lua_State *L)
{
	uint32_t ctypeid;
	const char *data = *(const char **) luaL_checkcdata(L, 1, &ctypeid);
	if (ctypeid != CTID_CHAR_PTR && ctypeid != CTID_CONST_CHAR_PTR)
		return luaL_error(L,
			"net_box.check_iproto_data: 'char *' or "
			"'const char *' expected");

	if (!lua_isnumber(L, 2))
		return luaL_error(L, "net_box.check_iproto_data: number "
				  "expected as 2nd argument");
	const char *end = data + lua_tointeger(L, 2);

	int ok = data < end &&
		mp_typeof(*data) == MP_MAP &&
		mp_check_map(data, end) <= 0 &&
		mp_decode_map(&data) == 1 &&
		data < end &&
		mp_typeof(*data) == MP_UINT &&
		mp_check_uint(data, end) <= 0 &&
		mp_decode_uint(&data) == IPROTO_DATA;

	if (!ok) {
		lua_pushnil(L);
		lua_pushstring(L,
			"net_box.check_iproto_data: wrong iproto data packet");
		return 2;
	}

	*(const char **) luaL_pushcdata(L, ctypeid) = data;
	return 1;
}

int
luaopen_net_box(struct lua_State *L)
{
	CTID_CHAR_PTR = luaL_ctypeid(L, "char *");
	assert(CTID_CHAR_PTR != 0);
	CTID_CONST_CHAR_PTR = luaL_ctypeid(L, "const char *");
	assert(CTID_CONST_CHAR_PTR != 0);

	static const luaL_Reg net_box_lib[] = {
		{ "encode_ping",    netbox_encode_ping },
		{ "encode_call_16", netbox_encode_call_16 },
		{ "encode_call",    netbox_encode_call },
		{ "encode_eval",    netbox_encode_eval },
		{ "encode_select",  netbox_encode_select },
		{ "encode_insert",  netbox_encode_insert },
		{ "encode_replace", netbox_encode_replace },
		{ "encode_delete",  netbox_encode_delete },
		{ "encode_update",  netbox_encode_update },
		{ "encode_upsert",  netbox_encode_upsert },
		{ "encode_execute", netbox_encode_execute},
		{ "encode_auth",    netbox_encode_auth },
		{ "decode_greeting",netbox_decode_greeting },
		{ "communicate",    netbox_communicate },
		{ "decode_select",  netbox_decode_select },
		{ "decode_execute", netbox_decode_execute },
		{ "check_iproto_data", netbox_check_iproto_data },
		{ NULL, NULL}
	};
	/* luaL_register_module polutes _G */
	lua_newtable(L);
	luaL_openlib(L, NULL, net_box_lib, 0);
	lua_getfield(L, LUA_REGISTRYINDEX, "_LOADED");
	lua_pushvalue(L, -2);
	lua_setfield(L, -2, "net.box.lib");
	lua_remove(L, -1);
	return 1;
}
