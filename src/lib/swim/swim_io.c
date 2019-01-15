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
#include "swim_io.h"
#include "swim_proto.h"
#include "fiber.h"

static ssize_t
swim_udp_transport_send(struct swim_transport *transport, const void *data,
			size_t size, const struct sockaddr *addr,
			socklen_t addr_size)
{
	ssize_t ret = sio_sendto(transport->fd, data, size, 0, addr, addr_size);
	if (ret == -1 && sio_wouldblock(errno))
		return 0;
	return ret;
}

static ssize_t
swim_udp_transport_recv(struct swim_transport *transport, void *buffer,
			size_t size, struct sockaddr *addr,
			socklen_t *addr_size)
{
	ssize_t ret = sio_recvfrom(transport->fd, buffer, size, 0, addr,
				   addr_size);
	if (ret == -1 && sio_wouldblock(errno))
		return 0;
	return ret;
}

static int
swim_udp_transport_bind(struct swim_transport *transport, struct sockaddr *addr,
			socklen_t addr_len)
{
	assert(addr->sa_family == AF_INET);
	const struct sockaddr_in *new_addr = (struct sockaddr_in *) addr;
	const struct sockaddr_in *old_addr = &transport->addr;
	assert(addr_len == sizeof(*new_addr));

	if (transport->fd != -1 &&
	    new_addr->sin_addr.s_addr == old_addr->sin_addr.s_addr &&
	    new_addr->sin_port == old_addr->sin_port)
		return 0;

	int fd = sio_socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (fd < 0)
		return -1;
	if (sio_bind(fd, addr, addr_len) != 0 ||
	    evio_setsockopt_server(fd, AF_INET, SOCK_DGRAM) != 0) {
		if (errno == EADDRINUSE)
			diag_set(SocketError, sio_socketname(fd), "bind");
		close(fd);
		return -1;
	}
	if (transport->fd != -1)
		close(transport->fd);
	transport->fd = fd;
	transport->addr = *new_addr;
	return 0;
}

static void
swim_udp_transport_destroy(struct swim_transport *transport)
{
	if (transport->fd != -1)
		close(transport->fd);
}

const struct swim_transport_vtab swim_udp_transport_vtab = {
	/* .send = */ swim_udp_transport_send,
	/* .recv = */ swim_udp_transport_recv,
	/* .bind = */ swim_udp_transport_bind,
	/* .destroy = */ swim_udp_transport_destroy,
};

struct swim_task *
swim_task_new(swim_task_f complete, void *ctx)
{
	struct swim_task *task = (struct swim_task *) malloc(sizeof(*task));
	if (task == NULL) {
		diag_set(OutOfMemory, sizeof(*task), "malloc", "task");
		return NULL;
	}
	swim_task_create(task, complete, ctx);
	task->is_static = false;
	return task;
}

void
swim_task_create(struct swim_task *task, swim_task_f complete, void *ctx)
{
	memset(task, 0, sizeof(*task));
	task->complete = complete;
	task->ctx = ctx;
	swim_packet_create(&task->packet);
	char *tmp = swim_packet_alloc(&task->packet,
				      sizeof(struct swim_meta_header_bin));
	assert(tmp != NULL);
	(void) tmp;
	rlist_create(&task->in_queue_output);
	task->is_static = true;
}

void
swim_task_schedule(struct swim_task *task, const struct sockaddr_in *dst,
		   struct swim_scheduler *scheduler)
{
	assert(! swim_task_is_active(task));
	task->dst = *dst;
	rlist_add_tail_entry(&scheduler->queue_output, task, in_queue_output);
	ev_io_start(loop(), &scheduler->output);
}

static void
swim_scheduler_on_output(struct ev_loop *loop, struct ev_io *io, int events);

static void
swim_scheduler_on_input(struct ev_loop *loop, struct ev_io *io, int events);

void
swim_scheduler_create(struct swim_scheduler *scheduler,
		      swim_scheduler_on_input_f on_input,
		      const struct swim_transport_vtab *transport_vtab)
{
	ev_init(&scheduler->output, swim_scheduler_on_output);
	scheduler->output.data = (void *) scheduler;
	ev_init(&scheduler->input, swim_scheduler_on_input);
	scheduler->input.data = (void *) scheduler;
	rlist_create(&scheduler->queue_output);
	scheduler->on_input = on_input;
	swim_transport_create(&scheduler->transport, transport_vtab);
}

int
swim_scheduler_bind(struct swim_scheduler *scheduler, struct sockaddr_in *addr)
{
	struct swim_transport *t = &scheduler->transport;
	if (swim_transport_bind(t, (struct sockaddr *) addr, sizeof(*addr)) != 0)
		return -1;
	ev_io_set(&scheduler->input, t->fd, EV_READ);
	ev_io_set(&scheduler->output, t->fd, EV_WRITE);
	return 0;
}

void
swim_scheduler_destroy(struct swim_scheduler *scheduler)
{
	struct swim_task *task, *tmp;
	rlist_foreach_entry_safe(task, &scheduler->queue_output,
				 in_queue_output, tmp) {
		if (! task->is_static)
			swim_task_delete(task);
	}
	swim_transport_destroy(&scheduler->transport);
	ev_io_stop(loop(), &scheduler->output);
	ev_io_stop(loop(), &scheduler->input);
}

static void
swim_scheduler_on_output(struct ev_loop *loop, struct ev_io *io, int events)
{
	assert((events & EV_WRITE) != 0);
	(void) events;
	struct swim_scheduler *scheduler = (struct swim_scheduler *) io->data;
	if (rlist_empty(&scheduler->queue_output)) {
		ev_io_stop(loop, io);
		return;
	}
	struct swim_task *task =
		rlist_shift_entry(&scheduler->queue_output, struct swim_task,
				  in_queue_output);
	say_verbose("SWIM: send to %s",
		    sio_strfaddr((struct sockaddr *) &task->dst,
				 sizeof(task->dst)));
	struct swim_meta_header_bin header;
	swim_meta_header_bin_create(&header, &scheduler->transport.addr);
	memcpy(task->packet.meta, &header, sizeof(header));
	int rc = swim_transport_send(&scheduler->transport, task->packet.body,
				     task->packet.pos - task->packet.body,
				     (const struct sockaddr *) &task->dst,
				     sizeof(task->dst));
	if (rc != 0)
		diag_log();
	if (task->complete != NULL)
		task->complete(task, rc);
	if (! task->is_static)
		swim_task_delete(task);
}

static void
swim_scheduler_on_input(struct ev_loop *loop, struct ev_io *io, int events)
{
	assert((events & EV_READ) != 0);
	(void) events;
	(void) loop;
	struct swim_scheduler *scheduler = (struct swim_scheduler *) io->data;
	struct sockaddr_in src;
	socklen_t len = sizeof(src);
	struct swim_packet packet;
	swim_packet_create(&packet);
	ssize_t size = swim_transport_recv(&scheduler->transport, packet.body,
					   packet.end - packet.body,
					   (struct sockaddr *) &src, &len);
	if (size <= 0) {
		if (size < 0)
			diag_log();
		return;
	}
	swim_packet_advance(&packet, size);
	swim_packet_flush(&packet);
	say_verbose("SWIM: received from %s",
		    sio_strfaddr((struct sockaddr *) &src, len));
	struct swim_meta_def meta;
	if (swim_meta_def_decode(&meta, (const char **) &packet.body,
				 packet.pos) < 0)
		return;
	scheduler->on_input(scheduler, &packet, &meta.src);
}
