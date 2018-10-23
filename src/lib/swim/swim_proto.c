/*
 * Copyright 2010-2019, Tarantool AUTHORS, please see AUTHORS file.
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
#include "swim_proto.h"
#include "msgpuck.h"
#include "say.h"
#include "version.h"

const char *swim_member_status_strs[] = {
	"alive",
	"dead",
};

void
swim_member_def_create(struct swim_member_def *def)
{
	memset(def, 0, sizeof(*def));
	def->status = MEMBER_ALIVE;
}

const char *swim_fd_msg_type_strs[] = {
	"ping",
	"ack",
};

/** Helper function to decode IP. */
static inline int
swim_decode_ip(const char **pos, const char *end, const char *msg_pref,
	       const char *param_name, struct sockaddr_in *address)
{
	if (mp_typeof(**pos) != MP_UINT || mp_check_uint(*pos, end) > 0) {
		say_error("%s %s should be uint", msg_pref, param_name);
		return -1;
	}
	uint64_t ip = mp_decode_uint(pos);
	if (ip > UINT32_MAX) {
		say_error("%s invalid IP address", msg_pref);
		return -1;
	}
	address->sin_addr.s_addr = ip;
	return 0;
}

/** Helper function to decode UDP port. */
static inline int
swim_decode_port(const char **pos, const char *end, const char *msg_pref,
		 const char *param_name, struct sockaddr_in *address)
{
	if (mp_typeof(**pos) != MP_UINT || mp_check_uint(*pos, end) > 0) {
		say_error("%s %s should be uint", msg_pref, param_name);
		return -1;
	}
	uint64_t port = mp_decode_uint(pos);
	if (port > UINT16_MAX) {
		say_error("%s %s is invalid", msg_pref, param_name);
		return -1;
	}
	address->sin_port = port;
	return 0;
}

/**
 * Decode a MessagePack value of @a key and store it in @a def.
 * @param key Key to read value of.
 * @param[in][out] pos Where a value is stored.
 * @param end End of the buffer.
 * @param msg_pref Error message prefix.
 * @param[out] def Where to store the value.
 *
 * @retval 0 Success.
 * @retval -1 Error.
 */
static int
swim_process_member_key(enum swim_member_key key, const char **pos,
			const char *end, const char *msg_pref,
			struct swim_member_def *def)
{
	uint64_t tmp;
	switch (key) {
	case SWIM_MEMBER_STATUS:
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s member status should be uint", msg_pref);
			return -1;
		}
		tmp = mp_decode_uint(pos);
		if (tmp >= swim_member_status_MAX) {
			say_error("%s unknown member status", msg_pref);
			return -1;
		}
		def->status = (enum swim_member_status) tmp;
		break;
	case SWIM_MEMBER_ADDRESS:
		if (swim_decode_ip(pos, end, msg_pref, "member address",
				   &def->addr) != 0)
			return -1;
		break;
	case SWIM_MEMBER_PORT:
		if (swim_decode_port(pos, end, msg_pref, "member port",
				     &def->addr) != 0)
			return -1;
		break;
	case SWIM_MEMBER_INCARNATION:
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s member incarnation should be uint",
				  msg_pref);
			return -1;
		}
		def->incarnation = mp_decode_uint(pos);
		break;
	default:
		unreachable();
	}
	return 0;
}

int
swim_member_def_decode(struct swim_member_def *def, const char **pos,
		       const char *end, const char *msg_pref)
{
	swim_member_def_create(def);
	uint64_t map_size = mp_decode_map(pos);
	for (uint64_t j = 0; j < map_size; ++j) {
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s member key should be uint", msg_pref);
			return -1;
		}
		uint64_t key = mp_decode_uint(pos);
		if (key >= swim_member_key_MAX) {
			say_error("%s unknown member key", msg_pref);
			return -1;
		}
		if (swim_process_member_key(key, pos, end, msg_pref, def) != 0)
			return -1;
	}
	if (def->addr.sin_port == 0 || def->addr.sin_addr.s_addr == 0) {
		say_error("%s member address should be specified", msg_pref);
		return -1;
	}
	return 0;
}

void
swim_fd_header_bin_create(struct swim_fd_header_bin *header,
			  enum swim_fd_msg_type type, uint64_t incarnation)
{
	header->k_header = SWIM_FAILURE_DETECTION;
	header->m_header = 0x82;

	header->k_type = SWIM_FD_MSG_TYPE;
	header->v_type = type;

	header->k_incarnation = SWIM_FD_INCARNATION;
	header->m_incarnation = 0xcf;
	header->v_incarnation = mp_bswap_u64(incarnation);
}

int
swim_failure_detection_def_decode(struct swim_failure_detection_def *def,
				  const char **pos, const char *end,
				  const char *msg_pref)
{
	if (mp_typeof(**pos) != MP_MAP || mp_check_map(*pos, end) > 0) {
		say_error("%s root should be a map", msg_pref);
		return -1;
	}
	memset(def, 0, sizeof(*def));
	def->type = swim_fd_msg_type_MAX;
	uint64_t size = mp_decode_map(pos);
	if (size != 2) {
		say_error("%s root map should have two keys - message type "\
			  "and incarnation", msg_pref);
		return -1;
	}
	for (int i = 0; i < (int) size; ++i) {
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s a key should be uint", msg_pref);
			return -1;
		}
		uint64_t key = mp_decode_uint(pos);
		switch(key) {
		case SWIM_FD_MSG_TYPE:
			if (mp_typeof(**pos) != MP_UINT ||
			    mp_check_uint(*pos, end) > 0) {
				say_error("%s message type should be uint",
					  msg_pref);
				return -1;
			}
			key = mp_decode_uint(pos);
			if (key >= swim_fd_msg_type_MAX) {
				say_error("%s unknown message type", msg_pref);
				return -1;
			}
			def->type = key;
			break;
		case SWIM_FD_INCARNATION:
			if (mp_typeof(**pos) != MP_UINT ||
			    mp_check_uint(*pos, end) > 0) {
				say_error("%s incarnation should be uint",
					  msg_pref);
				return -1;
			}
			def->incarnation = mp_decode_uint(pos);
			break;
		default:
			say_error("%s unknown key", msg_pref);
			return -1;
		}
	}
	if (def->type == swim_fd_msg_type_MAX) {
		say_error("%s message type should be specified", msg_pref);
		return -1;
	}
	return 0;
}

void
swim_anti_entropy_header_bin_create(struct swim_anti_entropy_header_bin *header,
				    int batch_size)
{
	header->k_anti_entropy = SWIM_ANTI_ENTROPY;
	header->m_anti_entropy = 0xdd;
	header->v_anti_entropy = mp_bswap_u32(batch_size);
}

void
swim_member_bin_fill(struct swim_member_bin *header,
		     enum swim_member_status status,
		     const struct sockaddr_in *addr, uint64_t incarnation)
{
	header->v_status = status;
	header->v_addr = mp_bswap_u32(addr->sin_addr.s_addr);
	header->v_port = mp_bswap_u16(addr->sin_port);
	header->v_incarnation = mp_bswap_u64(incarnation);
}

void
swim_member_bin_create(struct swim_member_bin *header)
{
	header->m_header = 0x84;
	header->k_status = SWIM_MEMBER_STATUS;
	header->k_addr = SWIM_MEMBER_ADDRESS;
	header->m_addr = 0xce;
	header->k_port = SWIM_MEMBER_PORT;
	header->m_port = 0xcd;
	header->k_incarnation = SWIM_MEMBER_INCARNATION;
	header->m_incarnation = 0xcf;
}

void
swim_meta_header_bin_create(struct swim_meta_header_bin *header,
			    const struct sockaddr_in *src)
{
	header->m_header = 0x83;
	header->k_version = SWIM_META_TARANTOOL_VERSION;
	header->m_version = 0xce;
	header->v_version = mp_bswap_u32(tarantool_version_id());
	header->k_addr = SWIM_META_SRC_ADDRESS;
	header->m_addr = 0xce;
	header->v_addr = mp_bswap_u32(src->sin_addr.s_addr);
	header->k_port = SWIM_META_SRC_PORT;
	header->m_port = 0xcd;
	header->v_port = mp_bswap_u16(src->sin_port);
}

int
swim_meta_def_decode(struct swim_meta_def *def, const char **pos,
		     const char *end)
{
	const char *msg_pref = "Invalid SWIM meta section:";
	if (mp_typeof(**pos) != MP_MAP || mp_check_map(*pos, end) > 0) {
		say_error("%s root should be a map", msg_pref);
		return -1;
	}
	uint64_t size = mp_decode_map(pos);
	memset(def, 0, sizeof(*def));
	for (uint64_t i = 0; i < size; ++i) {
		if (mp_typeof(**pos) != MP_UINT ||
		    mp_check_uint(*pos, end) > 0) {
			say_error("%s a key should be uint", msg_pref);
			return -1;
		}
		uint64_t key = mp_decode_uint(pos);
		switch (key) {
		case SWIM_META_TARANTOOL_VERSION:
			if (mp_typeof(**pos) != MP_UINT ||
			    mp_check_uint(*pos, end) > 0) {
				say_error("%s version should be uint",
					  msg_pref);
				return -1;
			}
			key = mp_decode_uint(pos);
			if (key > UINT32_MAX) {
				say_error("%s invalid version, too big",
					  msg_pref);
				return -1;
			}
			def->version = key;
			break;
		case SWIM_META_SRC_ADDRESS:
			if (swim_decode_ip(pos, end, msg_pref, "source address",
					   &def->src) != 0)
				return -1;
			break;
		case SWIM_META_SRC_PORT:
			if (swim_decode_port(pos, end, msg_pref, "source port",
					     &def->src) != 0)
				return -1;
			break;
		default:
			say_error("%s unknown key", msg_pref);
			return -1;
		}
	}
	if (def->version == 0) {
		say_error("%s version is mandatory", msg_pref);
		return -1;
	}
	if (def->src.sin_port == 0 || def->src.sin_addr.s_addr == 0) {
		say_error("%s source address should be specified", msg_pref);
		return -1;
	}
	return 0;
}
