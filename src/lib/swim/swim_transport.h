#ifndef TARANTOOL_SWIM_TRANSPORT_H_INCLUDED
#define TARANTOOL_SWIM_TRANSPORT_H_INCLUDED
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
#include <arpa/inet.h>

struct swim_transport;

typedef ssize_t (*swim_transport_send_f)(struct swim_transport *transport,
					 const void *data, size_t size,
					 const struct sockaddr *addr,
					 socklen_t addr_size);

typedef ssize_t (*swim_transport_recv_f)(struct swim_transport *transport,
					 void *buffer, size_t size,
					 struct sockaddr *addr,
					 socklen_t *addr_size);

typedef int (*swim_transport_bind_f)(struct swim_transport *transport,
				     struct sockaddr *addr, socklen_t addr_len);

typedef void (*swim_transport_destroy_f)(struct swim_transport *transport);

/**
 * Virtual methods of SWIM protocol steps. Usual implementation -
 * just libc analogues for all methods. But for testing via this
 * interface errors could be simulated.
 */
struct swim_transport_vtab {
	/**
	 * Send regular round message containing dissemination,
	 * failure detection and anti-entropy sections. Parameters
	 * are like sendto().
	 */
	swim_transport_send_f send;
	/**
	 * Receive a message. Not necessary round or failure
	 * detection. Before message is received, its type is
	 * unknown. Parameters are like recvfrom().
	 */
	swim_transport_recv_f recv;
	/** Bind the transport to an address. Just like bind(). */
	swim_transport_bind_f bind;
	/** Destructor. */
	swim_transport_destroy_f destroy;
};

/** Transport implementation. */
struct swim_transport {
	/** Socket. */
	int fd;
	/** Socket address. */
	struct sockaddr_in addr;
	/** Virtual methods. */
	const struct swim_transport_vtab *vtab;
};

static inline ssize_t
swim_transport_send(struct swim_transport *transport, const void *data,
		    size_t size, const struct sockaddr *addr,
		    socklen_t addr_size)
{
	return transport->vtab->send(transport, data, size, addr, addr_size);
}

static inline ssize_t
swim_transport_recv(struct swim_transport *transport, void *buffer, size_t size,
		    struct sockaddr *addr, socklen_t *addr_size)
{
	return transport->vtab->recv(transport, buffer, size, addr, addr_size);
}

static inline int
swim_transport_bind(struct swim_transport *transport, struct sockaddr *addr,
		    socklen_t addr_len)
{
	return transport->vtab->bind(transport, addr, addr_len);
}

static inline void
swim_transport_destroy(struct swim_transport *transport)
{
	transport->vtab->destroy(transport);
}

static inline void
swim_transport_create(struct swim_transport *transport,
		      const struct swim_transport_vtab *vtab)
{
	transport->fd = -1;
	transport->vtab = vtab;
}

/** UDP sendto/recvfrom implementation of swim_transport. */
extern const struct swim_transport_vtab swim_udp_transport_vtab;

#endif /* TARANTOOL_SWIM_TRANSPORT_H_INCLUDED */
