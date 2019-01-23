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

enum {
	/** Reserve 272 bytes for headers. */
	MAX_PAYLOAD_SIZE = 1200,
};

/**
 * SWIM binary protocol structures and helpers.
 */

enum swim_member_status {
	/**
	 * The instance is ok, it responds to requests, sends its
	 * members table.
	 */
	MEMBER_ALIVE = 0,
	/**
	 * The member is considered to be dead. It will disappear
	 * from the membership, if it is not pinned.
	 */
	MEMBER_DEAD,
	swim_member_status_MAX,
};

extern const char *swim_member_status_strs[];

/**
 * SWIM member attributes from anti-entropy and dissemination
 * messages.
 */
struct swim_member_def {
	struct sockaddr_in addr;
	uint64_t incarnation;
	enum swim_member_status status;
	const char *payload;
	int payload_size;
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
	SWIM_FAILURE_DETECTION,
	SWIM_DISSEMINATION,
};

/** {{{                Failure detection component              */

/** Possible failure detection keys. */
enum swim_fd_key {
	/** Type of the failure detection message: ping or ack. */
	SWIM_FD_MSG_TYPE,
	/**
	 * Incarnation of the sender. To make the member alive if
	 * it was considered to be dead, but ping/ack with greater
	 * incarnation was received from it.
	 */
	SWIM_FD_INCARNATION,
};

/** Failure detection message type. */
enum swim_fd_msg_type {
	SWIM_FD_MSG_PING,
	SWIM_FD_MSG_ACK,
	swim_fd_msg_type_MAX,
};

extern const char *swim_fd_msg_type_strs[];

/** SWIM failure detection MsgPack header template. */
struct PACKED swim_fd_header_bin {
	/** mp_encode_uint(SWIM_FAILURE_DETECTION) */
	uint8_t k_header;
	/** mp_encode_map(2) */
	uint8_t m_header;

	/** mp_encode_uint(SWIM_FD_MSG_TYPE) */
	uint8_t k_type;
	/** mp_encode_uint(enum swim_fd_msg_type) */
	uint8_t v_type;

	/** mp_encode_uint(SWIM_FD_INCARNATION) */
	uint8_t k_incarnation;
	/** mp_encode_uint(64bit incarnation) */
	uint8_t m_incarnation;
	uint64_t v_incarnation;
};

void
swim_fd_header_bin_create(struct swim_fd_header_bin *header,
			  enum swim_fd_msg_type type, uint64_t incarnation);

/** A decoded failure detection message. */
struct swim_failure_detection_def {
	/** Type of the message. */
	enum swim_fd_msg_type type;
	/** Incarnation of the sender. */
	uint64_t incarnation;
};

int
swim_failure_detection_def_decode(struct swim_failure_detection_def *def,
				  const char **pos, const char *end,
				  const char *msg_pref);

/** }}}               Failure detection component               */

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
	SWIM_MEMBER_INCARNATION,
	SWIM_MEMBER_PAYLOAD,
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
	/** mp_encode_map(5) */
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

	/** mp_encode_uint(SWIM_MEMBER_INCARNATION) */
	uint8_t k_incarnation;
	/** mp_encode_uint(64bit incarnation) */
	uint8_t m_incarnation;
	uint64_t v_incarnation;

	/** mp_encode_uint(SWIM_MEMBER_PAYLOAD) */
	uint8_t k_payload;
	/** mp_encode_bin(16bit bin header) */
	uint8_t m_payload_size;
	uint16_t v_payload_size;
	/** Payload data ... */
};

void
swim_member_bin_fill(struct swim_member_bin *header,
		     enum swim_member_status status,
		     const struct sockaddr_in *addr, uint64_t incarnation,
		     uint16_t payload_size);

void
swim_member_bin_create(struct swim_member_bin *header);

/** }}}                  Anti-entropy component                 */

/** {{{                 Dissemination component                 */

/** SWIM dissemination MsgPack template. */
struct PACKED swim_diss_header_bin {
	/** mp_encode_uint(SWIM_DISSEMINATION) */
	uint8_t k_header;
	/** mp_encode_array() */
	uint8_t m_header;
	uint32_t v_header;
};

void
swim_diss_header_bin_create(struct swim_diss_header_bin *header,
			    int batch_size);

/** SWIM event MsgPack template. */
struct PACKED swim_event_bin {
	/** mp_encode_map(4 or 5) */
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

	/** mp_encode_uint(SWIM_MEMBER_INCARNATION) */
	uint8_t k_incarnation;
	/** mp_encode_uint(64bit incarnation) */
	uint8_t m_incarnation;
	uint64_t v_incarnation;
};

void
swim_event_bin_create(struct swim_event_bin *header);

void
swim_event_bin_fill(struct swim_event_bin *header,
		    enum swim_member_status status,
		    const struct sockaddr_in *addr, uint64_t incarnation,
		    int payload_ttl);

/** }}}                 Dissemination component                 */

/** {{{                     Meta component                      */

enum swim_meta_key {
	SWIM_META_TARANTOOL_VERSION = 0,
	SWIM_META_SRC_ADDRESS,
	SWIM_META_SRC_PORT,
	SWIM_META_ROUTING,
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
	/** mp_encode_map(3 or 4) */
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
			    const struct sockaddr_in *src, bool has_routing);

/** Meta definition. */
struct swim_meta_def {
	/** Tarantool version. */
	uint32_t version;
	/** Source of the message. */
	struct sockaddr_in src;
	/** Route source and destination. */
	bool is_route_specified;
	struct {
		struct sockaddr_in src;
		struct sockaddr_in dst;
	} route;
};

int
swim_meta_def_decode(struct swim_meta_def *def, const char **pos,
		     const char *end);

enum swim_route_key {
	/**
	 * True source of the packet. Can be different from the
	 * packet sender. It is expected that the answer should
	 * be sent back to this address. Maybe indirectly through
	 * the same proxy.
	 */
	SWIM_ROUTE_SRC_ADDRESS = 0,
	SWIM_ROUTE_SRC_PORT,
	/**
	 * True destination of the packet. Can be different from
	 * this instance, receiver. If it is for another instance,
	 * then this packet is forwarded to the latter.
	 */
	SWIM_ROUTE_DST_ADDRESS,
	SWIM_ROUTE_DST_PORT,
	swim_route_key_MAX,
};

struct PACKED swim_route_bin {
	/** mp_encode_uint(SWIM_ROUTING) */
	uint8_t k_routing;
	/** mp_encode_map(4) */
	uint8_t m_routing;

	/** mp_encode_uint(SWIM_ROUTE_SRC_ADDRESS) */
	uint8_t k_src_addr;
	/** mp_encode_uint(addr.sin_addr.s_addr) */
	uint8_t m_src_addr;
	uint32_t v_src_addr;

	/** mp_encode_uint(SWIM_ROUTE_SRC_PORT) */
	uint8_t k_src_port;
	/** mp_encode_uint(addr.sin_port) */
	uint8_t m_src_port;
	uint16_t v_src_port;

	/** mp_encode_uint(SWIM_ROUTE_DST_ADDRESS) */
	uint8_t k_dst_addr;
	/** mp_encode_uint(addr.sin_addr.s_addr) */
	uint8_t m_dst_addr;
	uint32_t v_dst_addr;

	/** mp_encode_uint(SWIM_ROUTE_DST_PORT) */
	uint8_t k_dst_port;
	/** mp_encode_uint(addr.sin_port) */
	uint8_t m_dst_port;
	uint16_t v_dst_port;
};

void
swim_route_bin_create(struct swim_route_bin *route,
		      const struct sockaddr_in *src,
		      const struct sockaddr_in *dst);

/** }}}                     Meta component                      */

/**
 * SWIM message structure:
 * {
 *     SWIM_META_TARANTOOL_VERSION: uint, Tarantool version ID,
 *     SWIM_META_SRC_ADDRESS: uint, ip,
 *     SWIM_META_SRC_PORT: uint, port
 *     SWIM_META_ROUTING: {
 *         SWIM_ROUTE_SRC_ADDRESS: uint, ip,
 *         SWIM_ROUTE_SRC_PORT: uint, port,
 *         SWIM_ROUTE_DST_ADDRESS: uint, ip,
 *         SWIM_ROUTE_DST_PORT: uint, port
 *     }
 * }
 * {
 *     SWIM_FAILURE_DETECTION: {
 *         SWIM_FD_MSG_TYPE: uint, enum swim_fd_msg_type,
 *         SWIM_FD_INCARNATION: uint
 *     },
 *
 *                 OR/AND
 *
 *     SWIM_DISSEMINATION: [
 *         {
 *             SWIM_MEMBER_STATUS: uint, enum member_status,
 *             SWIM_MEMBER_ADDRESS: uint, ip,
 *             SWIM_MEMBER_PORT: uint, port,
 *             SWIM_MEMBER_INCARNATION: uint
 *         },
 *         ...
 *     ],
 *
 *                 OR/AND
 *
 *     SWIM_ANTI_ENTROPY: [
 *         {
 *             SWIM_MEMBER_STATUS: uint, enum member_status,
 *             SWIM_MEMBER_ADDRESS: uint, ip,
 *             SWIM_MEMBER_PORT: uint, port,
 *             SWIM_MEMBER_INCARNATION: uint
 *         },
 *         ...
 *     ],
 * }
 */

#endif /* TARANTOOL_SWIM_PROTO_H_INCLUDED */
