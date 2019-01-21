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
#include "relay.h"

#include "trivia/config.h"
#include "trivia/util.h"
#include "scoped_guard.h"
#include "cbus.h"
#include "cfg.h"
#include "errinj.h"
#include "fiber.h"
#include "say.h"

#include "coio.h"
#include "coio_task.h"
#include "engine.h"
#include "gc.h"
#include "iproto_constants.h"
#include "recovery.h"
#include "replication.h"
#include "trigger.h"
#include "vclock.h"
#include "version.h"
#include "xrow.h"
#include "xrow_io.h"
#include "xstream.h"
#include "wal.h"

enum {
	/**
	 * Send relay buffer if it's size reaches the threshold.
	 */
	RELAY_BUFFER_SEND_THRESHOLD = 8 * 1024,
};

/**
 * Cbus message to send status updates from relay to tx thread.
 */
struct relay_status_msg {
	/** Parent */
	struct cmsg msg;
	/** Relay instance */
	struct relay *relay;
	/** Replica vclock. */
	struct vclock vclock;
};

/** State of a replication relay. */
struct relay {
	/** The thread in which we relay data to the replica. */
	struct cord cord;
	/** Replica connection */
	struct ev_io io;
	/** Request sync */
	uint64_t sync;
	/** Recovery instance to read xlog from the disk */
	struct recovery *r;
	/** Xstream argument to recovery */
	struct xstream stream;
	/** Vclock to stop playing xlogs */
	struct vclock stop_vclock;
	/** Remote replica */
	struct replica *replica;
	/** WAL event watcher. */
	struct wal_watcher wal_watcher;
	/** Relay reader cond. */
	struct fiber_cond reader_cond;
	/** Relay diagnostics. */
	struct diag diag;
	/** Vclock recieved from replica. */
	struct vclock recv_vclock;
	/** Replicatoin slave version. */
	uint32_t version_id;
	/**
	 * Local vclock at the moment of subscribe, used to check
	 * dataset on the other side and send missing data rows if any.
	 */
	struct vclock local_vclock_at_subscribe;
	/** Cached wal_dir cfg option. */
	const char *wal_dir;

	/** Time when last row was sent to peer. */
	double last_row_tm;
	/** Relay sync state. */
	enum relay_state state;

	struct {
		/* Align to prevent false-sharing with tx thread */
		alignas(CACHELINE_SIZE)
		/** Known relay vclock. */
		struct vclock vclock;
	} tx;
	/** Buffer to accumulate rows before sending. */
	struct ibuf send_buf;
};

struct diag*
relay_get_diag(struct relay *relay)
{
	return &relay->diag;
}

enum relay_state
relay_get_state(const struct relay *relay)
{
	return relay->state;
}

const struct vclock *
relay_vclock(const struct relay *relay)
{
	return &relay->tx.vclock;
}

static void
relay_send(struct relay *relay, struct xrow_header *packet);
static void
relay_send_initial_join_row(struct xstream *stream, struct xrow_header *row);
static void
relay_send_row(struct xstream *stream, struct xrow_header *row);

struct relay *
relay_new(struct replica *replica)
{
	struct relay *relay = (struct relay *) calloc(1, sizeof(struct relay));
	if (relay == NULL) {
		diag_set(OutOfMemory, sizeof(struct relay), "malloc",
			  "struct relay");
		return NULL;
	}
	relay->replica = replica;
	fiber_cond_create(&relay->reader_cond);
	diag_create(&relay->diag);
	relay->state = RELAY_OFF;
	return relay;
}

static void
relay_start(struct relay *relay, int fd, uint64_t sync,
	     void (*stream_write)(struct xstream *, struct xrow_header *))
{
	xstream_create(&relay->stream, stream_write);
	/*
	 * Clear the diagnostics at start, in case it has the old
	 * error message which we keep around to display in
	 * box.info.replication.
	 */
	diag_clear(&relay->diag);
	coio_create(&relay->io, fd);
	relay->sync = sync;
	relay->state = RELAY_FOLLOW;
}

void
relay_cancel(struct relay *relay)
{
	/* Check that the thread is running first. */
	if (relay->cord.id != 0) {
		if (tt_pthread_cancel(relay->cord.id) == ESRCH)
			return;
		tt_pthread_join(relay->cord.id, NULL);
	}
}

/**
 * Called by a relay thread right before termination.
 */
static void
relay_exit(struct relay *relay)
{
	struct errinj *inj = errinj(ERRINJ_RELAY_EXIT_DELAY, ERRINJ_DOUBLE);
	if (inj != NULL && inj->dparam > 0)
		fiber_sleep(inj->dparam);

	/*
	 * Destroy the recovery context. We MUST do it in
	 * the relay thread, because it contains an xlog
	 * cursor, which must be closed in the same thread
	 * that opened it (it uses cord's slab allocator).
	 */
	if (relay->r)
		recovery_delete(relay->r);
	relay->r = NULL;
}

static void
relay_stop(struct relay *relay)
{
	if (relay->r != NULL)
		recovery_delete(relay->r);
	relay->r = NULL;
	relay->state = RELAY_STOPPED;
	/*
	 * Needed to track whether relay thread is running or not
	 * for relay_cancel(). Id is reset to a positive value
	 * upon cord_create().
	 */
	relay->cord.id = 0;
}

void
relay_delete(struct relay *relay)
{
	if (relay->state == RELAY_FOLLOW)
		relay_stop(relay);
	fiber_cond_destroy(&relay->reader_cond);
	diag_destroy(&relay->diag);
	TRASH(relay);
	free(relay);
}

static void
relay_set_cord_name(int fd)
{
	char name[FIBER_NAME_MAX];
	struct sockaddr_storage peer;
	socklen_t addrlen = sizeof(peer);
	if (getpeername(fd, ((struct sockaddr*)&peer), &addrlen) == 0) {
		snprintf(name, sizeof(name), "relay/%s",
			 sio_strfaddr((struct sockaddr *)&peer, addrlen));
	} else {
		snprintf(name, sizeof(name), "relay/<unknown>");
	}
	cord_set_name(name);
}

static void
relay_flush(struct relay *relay)
{
	if (ibuf_used(&relay->send_buf) == 0)
		return;
	/* Send accumulated data. */
	coio_write(&relay->io, relay->send_buf.rpos,
		   ibuf_used(&relay->send_buf));
	ibuf_reset(&relay->send_buf);
}

void
relay_initial_join(int fd, uint64_t sync, struct vclock *vclock)
{
	struct relay *relay = relay_new(NULL);
	if (relay == NULL)
		diag_raise();

	relay_start(relay, fd, sync, relay_send_initial_join_row);
	auto relay_guard = make_scoped_guard([=] {
		relay_stop(relay);
		relay_delete(relay);
	});

	engine_join_xc(vclock, &relay->stream);
}

int
relay_final_join_f(va_list ap)
{
	struct relay *relay = va_arg(ap, struct relay *);
	auto guard = make_scoped_guard([=] { relay_exit(relay); });

	coio_enable();
	relay_set_cord_name(relay->io.fd);
	ibuf_create(&relay->send_buf, &cord()->slabc,
		    2 * RELAY_BUFFER_SEND_THRESHOLD);

	/* Send all WALs until stop_vclock */
	assert(relay->stream.write != NULL);
	recover_remaining_wals(relay->r, &relay->stream,
			       &relay->stop_vclock, true);
	relay_flush(relay);
	assert(vclock_compare(&relay->r->vclock, &relay->stop_vclock) == 0);
	ibuf_destroy(&relay->send_buf);
	return 0;
}

void
relay_final_join(int fd, uint64_t sync, struct vclock *start_vclock,
		 struct vclock *stop_vclock)
{
	struct relay *relay = relay_new(NULL);
	if (relay == NULL)
		diag_raise();

	relay_start(relay, fd, sync, relay_send_row);
	auto relay_guard = make_scoped_guard([=] {
		relay_stop(relay);
		relay_delete(relay);
	});

	relay->r = recovery_new(cfg_gets("wal_dir"), false,
			       start_vclock);
	vclock_copy(&relay->stop_vclock, stop_vclock);

	int rc = cord_costart(&relay->cord, "final_join",
			      relay_final_join_f, relay);
	if (rc == 0)
		rc = cord_cojoin(&relay->cord);
	if (rc != 0)
		diag_raise();

	ERROR_INJECT(ERRINJ_RELAY_FINAL_JOIN,
		     tnt_raise(ClientError, ER_INJECTION, "relay final join"));

	ERROR_INJECT(ERRINJ_RELAY_FINAL_SLEEP, {
		while (vclock_compare(stop_vclock, &replicaset.vclock) == 0)
			fiber_sleep(0.001);
	});
}

/**
 * The message which updated tx thread with a new vclock has returned back
 * to the relay.
 */
int
relay_status_update(struct replica *replica, struct vclock *vclock)
{
	struct relay *relay = replica->relay;
	vclock_copy(&relay->tx.vclock, vclock);
	if (vclock_compare(vclock, &replica->gc->vclock) == 1)
		gc_consumer_advance(replica->gc, vclock);
	return 0;
}

int
relay_recover_wals(struct replica *replica, struct recovery *recovery)
{
	struct relay *relay = replica->relay;
	ibuf_create(&relay->send_buf, &cord()->slabc,
		    2 * RELAY_BUFFER_SEND_THRESHOLD);

	int res = 0;
	try {
		recover_remaining_wals(recovery, &relay->stream, NULL, true);
		relay_flush(relay);
	} catch (Exception &ex) {
		res = -1;
	}

	ibuf_destroy(&relay->send_buf);
	return res;
}

/** Replication acceptor fiber handler. */
void
relay_subscribe(struct replica *replica, int fd, uint64_t sync,
		struct vclock *replica_clock, uint32_t replica_version_id)
{
	assert(replica->id != REPLICA_ID_NIL);
	struct relay *relay = replica->relay;
	assert(relay->state != RELAY_FOLLOW);
	/*
	 * Register the replica with the garbage collector
	 * unless it has already been registered by initial
	 * join.
	 */
	if (replica->gc == NULL) {
		replica->gc = gc_consumer_register(replica_clock, "replica %s",
						   tt_uuid_str(&replica->uuid));
		if (replica->gc == NULL)
			diag_raise();
	}

	relay_start(relay, fd, sync, relay_send_row);
	vclock_copy(&relay->local_vclock_at_subscribe, &replicaset.vclock);
	relay->wal_dir = cfg_gets("wal_dir");
	relay->r = NULL;
	vclock_copy(&relay->tx.vclock, replica_clock);
	relay->version_id = replica_version_id;

	wal_relay(replica, replica_clock, fd);

	relay_exit(relay);
	relay_stop(relay);
	replica_on_relay_stop(replica);

	if (!diag_is_empty(&fiber()->diag)) {
		if (diag_is_empty(&relay->diag))
			diag_add_error(&relay->diag, diag_last_error(&fiber()->diag));
		diag_raise();
	}
}

static void
relay_send_buffered(struct relay *relay, struct xrow_header *packet)
{
	struct errinj *inj = errinj(ERRINJ_RELAY_SEND_DELAY, ERRINJ_BOOL);
	while (inj != NULL && inj->bparam) {
		relay_flush(relay);
		fiber_sleep(0.01);
	}

	packet->sync = relay->sync;
	relay->last_row_tm = ev_monotonic_now(loop());
	/* Dump row to send buffer. */
	struct iovec iov[XROW_IOVMAX];
	int iovcnt = xrow_to_iovec_xc(packet, iov);
	int i;
	for (i = 0; i < iovcnt; ++i) {
		void *p = ibuf_alloc(&relay->send_buf, iov[i].iov_len);
		if (p == NULL)
			tnt_raise(OutOfMemory, iov[i].iov_len, "region",
				  "xrow");
		memcpy(p, iov[i].iov_base, iov[i].iov_len);
	}
	if (ibuf_used(&relay->send_buf) >= RELAY_BUFFER_SEND_THRESHOLD)
		relay_flush(relay);
	fiber_gc();

	inj = errinj(ERRINJ_RELAY_TIMEOUT, ERRINJ_DOUBLE);
	if (inj != NULL && inj->dparam > 0) {
		relay_flush(relay);
		fiber_sleep(inj->dparam);
	}
}

static void
relay_send(struct relay *relay, struct xrow_header *packet)
{
	struct errinj *inj = errinj(ERRINJ_RELAY_SEND_DELAY, ERRINJ_BOOL);
	while (inj != NULL && inj->bparam)
		fiber_sleep(0.01);

	packet->sync = relay->sync;
	relay->last_row_tm = ev_monotonic_now(loop());
	coio_write_xrow(&relay->io, packet);
	fiber_gc();

	inj = errinj(ERRINJ_RELAY_TIMEOUT, ERRINJ_DOUBLE);
	if (inj != NULL && inj->dparam > 0)
		fiber_sleep(inj->dparam);
}

static void
relay_send_initial_join_row(struct xstream *stream, struct xrow_header *row)
{
	struct relay *relay = container_of(stream, struct relay, stream);
	/*
	 * Ignore replica local requests as we don't need to promote
	 * vclock while sending a snapshot.
	 */
	if (row->group_id != GROUP_LOCAL)
		relay_send(relay, row);
}

/** Send a single row to the client. */
static void
relay_send_row(struct xstream *stream, struct xrow_header *packet)
{
	struct relay *relay = container_of(stream, struct relay, stream);
	assert(iproto_type_is_dml(packet->type));
	/*
	 * Transform replica local requests to IPROTO_NOP so as to
	 * promote vclock on the replica without actually modifying
	 * any data.
	 */
	if (packet->group_id == GROUP_LOCAL) {
		packet->type = IPROTO_NOP;
		packet->group_id = GROUP_DEFAULT;
		packet->bodycnt = 0;
	}
	/*
	 * We're feeding a WAL, thus responding to FINAL JOIN or SUBSCRIBE
	 * request. If this is FINAL JOIN (i.e. relay->replica is NULL),
	 * we must relay all rows, even those originating from the replica
	 * itself (there may be such rows if this is rebootstrap). If this
	 * SUBSCRIBE, only send a row if it is not from the same replica
	 * (i.e. don't send replica's own rows back) or if this row is
	 * missing on the other side (i.e. in case of sudden power-loss,
	 * data was not written to WAL, so remote master can't recover
	 * it). In the latter case packet's LSN is less than or equal to
	 * local master's LSN at the moment it received 'SUBSCRIBE' request.
	 */
	if (relay->replica == NULL ||
	    packet->replica_id != relay->replica->id ||
	    packet->lsn <= vclock_get(&relay->local_vclock_at_subscribe,
				      packet->replica_id)) {
		struct errinj *inj = errinj(ERRINJ_RELAY_BREAK_LSN,
					    ERRINJ_INT);
		if (inj != NULL && packet->lsn == inj->iparam) {
			packet->lsn = inj->iparam - 1;
			say_warn("injected broken lsn: %lld",
				 (long long) packet->lsn);
		}
		relay_send_buffered(relay, packet);
	}
}
