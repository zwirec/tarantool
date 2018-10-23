#ifndef TARANTOOL_SWIM_PROTO_H_INCLUDED
#define TARANTOOL_SWIM_PROTO_H_INCLUDED
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
#include "trivia/util.h"
#include <arpa/inet.h>
#include <stdbool.h>

/**
 * SWIM binary protocol structures and helpers.
 */

enum swim_member_status {
	/**
	 * The instance is ok, it responds to requests, sends its
	 * members table.
	 */
	MEMBER_ALIVE = 0,
	swim_member_status_MAX,
};

extern const char *swim_member_status_strs[];

/**
 * SWIM member attributes from anti-entropy and dissemination
 * messages.
 */
struct swim_member_def {
	struct sockaddr_in addr;
	enum swim_member_status status;
};

void
swim_member_def_create(struct swim_member_def *def);

int
swim_member_def_decode(struct swim_member_def *def, const char **pos,
		       const char *end, const char *msg_pref);

/**
 * Main round messages can carry merged failure detection
 * messages and anti-entropy. With these keys the components can
 * be distinguished from each other.
 */
enum swim_component_type {
	SWIM_ANTI_ENTROPY = 0,
};

/** {{{                  Anti-entropy component                 */

/**
 * Attributes of each record of a broadcasted member table. Just
 * the same as some of struct swim_member attributes.
 */
enum swim_member_key {
	SWIM_MEMBER_STATUS = 0,
	/**
	 * Now can only be IP. But in future UNIX sockets can be
	 * added.
	 */
	SWIM_MEMBER_ADDRESS,
	SWIM_MEMBER_PORT,
	swim_member_key_MAX,
};

/** SWIM anti-entropy MsgPack header template. */
struct PACKED swim_anti_entropy_header_bin {
	/** mp_encode_uint(SWIM_ANTI_ENTROPY) */
	uint8_t k_anti_entropy;
	/** mp_encode_array() */
	uint8_t m_anti_entropy;
	uint32_t v_anti_entropy;
};

void
swim_anti_entropy_header_bin_create(struct swim_anti_entropy_header_bin *header,
				    int batch_size);

/** SWIM member MsgPack template. */
struct PACKED swim_member_bin {
	/** mp_encode_map(3) */
	uint8_t m_header;

	/** mp_encode_uint(SWIM_MEMBER_STATUS) */
	uint8_t k_status;
	/** mp_encode_uint(enum member_status) */
	uint8_t v_status;

	/** mp_encode_uint(SWIM_MEMBER_ADDRESS) */
	uint8_t k_addr;
	/** mp_encode_uint(addr.sin_addr.s_addr) */
	uint8_t m_addr;
	uint32_t v_addr;

	/** mp_encode_uint(SWIM_MEMBER_PORT) */
	uint8_t k_port;
	/** mp_encode_uint(addr.sin_port) */
	uint8_t m_port;
	uint16_t v_port;
};

void
swim_member_bin_fill(struct swim_member_bin *header,
		     enum swim_member_status status,
		     const struct sockaddr_in *addr);

void
swim_member_bin_create(struct swim_member_bin *header);

/** }}}                  Anti-entropy component                 */

/** {{{                     Meta component                      */

enum swim_meta_key {
	SWIM_META_TARANTOOL_VERSION = 0,
	SWIM_META_SRC_ADDRESS,
	SWIM_META_SRC_PORT,
};

/**
 * Each SWIM packet carries meta info, which helps to determine
 * SWIM protocol version, final packet destination and any other
 * internal details, not linked with etalon SWIM protocol.
 *
 * The meta header is mandatory, preceeds main protocol data, and
 * contains at least Tarantool version.
 */
struct PACKED swim_meta_header_bin {
	/** mp_encode_map(3) */
	uint8_t m_header;

	/** mp_encode_uint(SWIM_META_TARANTOOL_VERSION) */
	uint8_t k_version;
	/** mp_encode_uint(tarantool_version_id()) */
	uint8_t m_version;
	uint32_t v_version;

	/** mp_encode_uint(SWIM_META_SRC_ADDRESS) */
	uint8_t k_addr;
	/** mp_encode_uint(addr.sin_addr.s_addr) */
	uint8_t m_addr;
	uint32_t v_addr;

	/** mp_encode_uint(SWIM_META_SRC_PORT) */
	uint8_t k_port;
	/** mp_encode_uint(addr.sin_port) */
	uint8_t m_port;
	uint16_t v_port;
};

void
swim_meta_header_bin_create(struct swim_meta_header_bin *header,
			    const struct sockaddr_in *src);

/** Meta definition. */
struct swim_meta_def {
	/** Tarantool version. */
	uint32_t version;
	/** Source of the message. */
	struct sockaddr_in src;
};

int
swim_meta_def_decode(struct swim_meta_def *def, const char **pos,
		     const char *end);

/** }}}                     Meta component                      */

/**
 * SWIM message structure:
 * {
 *     SWIM_META_TARANTOOL_VERSION: uint, Tarantool version ID,
 *     SWIM_META_SRC_ADDRESS: uint, ip,
 *     SWIM_META_SRC_PORT: uint, port
 * }
 * {
 *     SWIM_ANTI_ENTROPY: [
 *         {
 *             SWIM_MEMBER_STATUS: uint, enum member_status,
 *             SWIM_MEMBER_ADDRESS: uint, ip,
 *             SWIM_MEMBER_PORT: uint, port
 *         },
 *         ...
 *     ],
 * }
 */

#endif /* TARANTOOL_SWIM_PROTO_H_INCLUDED */
