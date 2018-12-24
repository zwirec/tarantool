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
#include "wal.h"

#include "vclock.h"
#include "fiber.h"
#include "fio.h"
#include "errinj.h"
#include "error.h"
#include "exception.h"

#include "xlog.h"
#include "xrow.h"
#include "vy_log.h"
#include "cbus.h"
#include "coio.h"
#include "coio_task.h"
#include "replication.h"
#include "recovery.h"
#include "cfg.h"
#include "xstream.h"
#include "xrow_io.h"
#include "iproto_constants.h"

enum {
	/**
	 * Size of disk space to preallocate with xlog_fallocate().
	 * Obviously, we want to call this function as infrequent as
	 * possible to avoid the overhead associated with a system
	 * call, however at the same time we do not want to call it
	 * to allocate too big chunks, because this may increase tx
	 * latency. 1 MB seems to be a well balanced choice.
	 */
	WAL_FALLOCATE_LEN = 1024 * 1024,
};

const char *wal_mode_STRS[] = { "none", "write", "fsync", NULL };

int wal_dir_lock = -1;

static int64_t
wal_write(struct journal *, struct journal_entry *);

static int64_t
wal_write_in_wal_mode_none(struct journal *, struct journal_entry *);

/* WAL thread. */
struct wal_thread {
	/** 'wal' thread doing the writes. */
	struct cord cord;
	/** A pipe from 'tx' thread to 'wal' */
	struct cpipe wal_pipe;
	/**
	 * Return pipe from 'wal' to tx'. This is a
	 * priority pipe and DOES NOT support yield.
	 */
	struct cpipe tx_prio_pipe;
};

/*
 * WAL writer - maintain a Write Ahead Log for every change
 * in the data state.
 *
 * @sic the members are arranged to ensure proper cache alignment,
 * members used mainly in tx thread go first, wal thread members
 * following.
 */

struct wal_buf_item {
	struct vclock vclock;
	uint64_t pos;
	uint64_t size;
	uint8_t buf_no;
};

struct wal_status_msg {
	struct cmsg m;
	struct vclock wal_vclock;
	struct vclock commit_vclock;
	struct fiber_cond done_cond;
};

struct wal_writer
{
	struct journal base;
	/* ----------------- tx ------------------- */
	/**
	 * The rollback queue. An accumulator for all requests
	 * that need to be rolled back. Also acts as a valve
	 * in wal_write() so that new requests never enter
	 * the wal-tx bus and are rolled back "on arrival".
	 */
	struct stailq rollback;
	/* ----------------- wal ------------------- */
	/** A setting from instance configuration - rows_per_wal */
	int64_t wal_max_rows;
	/** A setting from instance configuration - wal_max_size */
	int64_t wal_max_size;
	/** Another one - wal_mode */
	enum wal_mode wal_mode;
	/** wal_dir, from the configuration file. */
	struct xdir wal_dir;
	/**
	 * The vector clock of the WAL writer. It's a bit behind
	 * the vector clock of the transaction thread, since it
	 * "follows" the tx vector clock.
	 * By "following" we mean this: whenever a transaction
	 * is started in 'tx' thread, it's assigned a tentative
	 * LSN. If the transaction is rolled back, this LSN
	 * is abandoned. Otherwise, after the transaction is written
	 * to the log with this LSN, WAL writer vclock is advanced
	 * with this LSN and LSN becomes "real".
	 */
	struct vclock vclock;
	/**
	 * Signature of the oldest checkpoint available on the instance.
	 * The WAL writer must not delete WAL files that are needed to
	 * recover from it even if it is running out of disk space.
	 */
	int64_t checkpoint_lsn;
	/** The current WAL file. */
	struct xlog current_wal;
	/**
	 * WAL watchers, i.e. threads that should be alerted
	 * whenever there are new records appended to the journal.
	 * Used for replication relays.
	 */
	struct rlist watchers;

	/** Commit facilities */
	struct stailq queue;
	struct fiber_cond commit_cond;
	struct fiber *commit_fiber;
	struct vclock commit_vclock;

	/** Rollback facilities */
	bool in_rollback;
	struct journal_entry *last_entry;
	struct fiber_cond rollback_cond;

	struct rlist relay;

	struct ibuf wal_buf_index;
	struct ibuf wal_buf[2];

	/** Status transfer facilities */
	struct fiber *status_fiber;
	struct fiber_cond status_cond;
	struct wal_status_msg status_msg;
	struct rlist on_wal_status;

};

struct wal_msg {
	struct cmsg base;
	/** Approximate size of this request when encoded. */
	size_t approx_len;
	/** Input queue, on output contains all committed requests. */
	struct stailq commit;
};

#define WAL_RELAY_ONLINE   1
#define WAL_RELAY_MEM      2
#define WAL_RELAY_FILE     3
#define WAL_RELAY_ERROR    4

struct wal_relay {
	struct rlist item;
	uint32_t state;
	struct vclock send_vclock;
	struct vclock recv_vclock;
	struct replica *replica;
	struct fiber_cond online;
	void *send_buf;
	uint32_t to_send;
	struct ev_io io;
	struct fiber *fiber;
};

/**
 * Vinyl metadata log writer.
 */
struct vy_log_writer {
	/** The metadata log file. */
	struct xlog xlog;
};

static struct vy_log_writer vy_log_writer;
static struct wal_thread wal_thread;
static struct wal_writer wal_writer_singleton;

enum wal_mode
wal_mode()
{
	return wal_writer_singleton.wal_mode;
}

static void
wal_write_to_disk(struct cmsg *msg);

/**
 * Invoke fibers waiting for their journal_entry's to be
 * completed. The fibers are invoked in strict fifo order:
 * this ensures that, in case of rollback, requests are
 * rolled back in strict reverse order, producing
 * a consistent database state.
 */
static void
tx_schedule_queue(struct stailq *queue)
{
	/*
	 * fiber_wakeup() is faster than fiber_call() when there
	 * are many ready fibers.
	 */
	struct journal_entry *req;
	stailq_foreach_entry(req, queue, fifo)
		fiber_wakeup(req->fiber);
}

static struct cmsg_hop wal_request_route[] = {
	{wal_write_to_disk, NULL}
};

struct wal_done_msg {
	struct cmsg m;
	struct stailq queue;
	struct vclock vclock;
};

static void
tx_wal_request_commit(struct cmsg *m)
{
	struct wal_done_msg *msg = container_of(m, struct wal_done_msg, m);
	tx_schedule_queue(&msg->queue);
//FIXME: Cross thread free
	free(msg);
}

struct cmsg_hop wal_request_commit[] = {
	{tx_wal_request_commit, NULL}
};

/*
 * Rollback facilities
 */

static void
wal_writer_end_rollback(struct cmsg *msg)
{
	(void) msg;
	struct wal_writer *writer = &wal_writer_singleton;
	writer->in_rollback = false;
}

static struct cmsg_hop wal_rollback_done[] = {
	{wal_writer_end_rollback, NULL}
};

static void
tx_wal_request_rollback(struct cmsg *m)
{
	struct wal_done_msg *msg = container_of(m, struct wal_done_msg, m);
	/*
	* Move the rollback list to the writer first, since
	* wal_msg memory disappears after the first
	* iteration of tx_schedule_queue loop.
	*/
	struct wal_writer *writer = &wal_writer_singleton;
	stailq_concat(&writer->rollback, &msg->queue);
	if (stailq_last_entry(&writer->rollback, struct journal_entry, fifo) ==
	    writer->last_entry) {
	       /*
		* Perform a cascading abort of all transactions which
		* depend on the transaction which failed to get written
		* to the write ahead log. Abort transactions
		* in reverse order, performing a playback of the
		* in-memory database state.
		*/
		stailq_reverse(&writer->rollback);
		/* Must not yield. */
		tx_schedule_queue(&writer->rollback);

		static struct cmsg msg;
		cmsg_init(&msg, wal_rollback_done);
		cpipe_push(&wal_thread.wal_pipe, &msg);
	}
//FIXME: Cross-thread free
	free(msg);
}

struct cmsg_hop wal_request_rollback[] = {
	{tx_wal_request_rollback, NULL}
};

static void
wal_queue_rollback(struct stailq *queue)
{
	struct wal_writer *writer = &wal_writer_singleton;
	writer->in_rollback = true;

	struct journal_entry *entry;
	stailq_foreach_entry(entry, queue, fifo)
		entry->res = -1;

//FIXME: Cross-thread alloc
	struct wal_done_msg *msg = (struct wal_done_msg *)
		malloc(sizeof(struct wal_done_msg));
	cmsg_init(&msg->m, wal_request_rollback);
	stailq_concat(&msg->queue, queue);
	cpipe_push(&wal_thread.tx_prio_pipe, &msg->m);
}

/*
 * End of rollback facilities.
 */

static void
wal_msg_create(struct wal_msg *batch)
{
	cmsg_init(&batch->base, wal_request_route);
	batch->approx_len = 0;
	stailq_create(&batch->commit);
}

static struct wal_msg *
wal_msg(struct cmsg *msg)
{
	return msg->route == wal_request_route ? (struct wal_msg *) msg : NULL;
}

static int
wal_commit_f(va_list ap)
{
	(void) ap;
	struct wal_writer *writer = &wal_writer_singleton;
	while (!fiber_is_cancelled()) {
		fiber_cond_wait(&writer->commit_cond);

		struct wal_done_msg *msg = (struct wal_done_msg *)
			malloc(sizeof(struct wal_done_msg));
		stailq_create(&msg->queue);

		while (!stailq_empty(&writer->queue)) {
			struct journal_entry *entry;
			entry = stailq_first_entry(&writer->queue,
						   struct journal_entry, fifo);
			int64_t txn = (*entry->rows)->txn;
			uint32_t txn_replica_id = (*entry->rows)->txn_replica_id;
			if (txn > vclock_get(&writer->commit_vclock,
					     txn_replica_id))
				break;
			stailq_add(&msg->queue, stailq_shift(&writer->queue));
		}

		vclock_copy(&msg->vclock, &writer->vclock);

		if (stailq_empty(&msg->queue))
			continue;
		cmsg_init(&msg->m, wal_request_commit);
		cpipe_push(&wal_thread.tx_prio_pipe, &msg->m);
	}
	return 0;
}

static void
tx_update_wal_status(struct cmsg *m)
{
	struct wal_writer *writer = &wal_writer_singleton;
	struct wal_status_msg *msg = container_of(m, struct wal_status_msg, m);
	vclock_copy(&replicaset.vclock, &msg->commit_vclock);
	vclock_copy(&replicaset.wal_vclock, &msg->wal_vclock);
	say_error("update stat");
	trigger_run(&writer->on_wal_status, &msg->wal_vclock);
}

static void
wal_update_wal_status(struct cmsg *m)
{
	struct wal_status_msg *msg = container_of(m, struct wal_status_msg, m);
	say_error("wake not");
	fiber_cond_signal(&msg->done_cond);
}

static int
wal_status_f(va_list ap)
{
	(void) ap;
	static const struct cmsg_hop status_route[] = {
		{tx_update_wal_status, &wal_thread.wal_pipe},
		{wal_update_wal_status, NULL}
	};
	struct wal_writer *writer = &wal_writer_singleton;
	struct wal_status_msg *msg = &writer->status_msg;
	fiber_cond_create(&msg->done_cond);
	while (!fiber_is_cancelled()) {
		if (vclock_compare(&msg->wal_vclock, &writer->vclock) == 0)
			fiber_cond_wait(&writer->status_cond);
		vclock_copy(&msg->wal_vclock, &writer->vclock);
		vclock_copy(&msg->commit_vclock, &writer->commit_vclock);
		cmsg_init(&msg->m, status_route);
		say_error("push notify");
		cpipe_push(&wal_thread.tx_prio_pipe, &msg->m);
		fiber_cond_wait(&writer->status_msg.done_cond);
		say_error("end notify");
	}
	return 0;
}

void
on_wal_status(struct trigger *trigger)
{
	struct wal_writer *writer = &wal_writer_singleton;
	trigger_add(&writer->on_wal_status, trigger);
}

/** Write a request to a log in a single transaction. */
static ssize_t
xlog_write_entry(struct xlog *l, struct journal_entry *entry)
{
	/*
	 * Iterate over request rows (tx statements)
	 */
	xlog_tx_begin(l);
	struct xrow_header **row = entry->rows;
	for (; row < entry->rows + entry->n_rows; row++) {
		struct errinj *inj = errinj(ERRINJ_WAL_BREAK_LSN, ERRINJ_INT);
		if (inj != NULL && inj->iparam == (*row)->lsn) {
			(*row)->lsn = inj->iparam - 1;
			say_warn("injected broken lsn: %lld",
				 (long long) (*row)->lsn);
		}
		if (xlog_write_row(l, *row) < 0) {
			/*
			 * Rollback all un-written rows
			 */
			xlog_tx_rollback(l);
			return -1;
		}
	}
	return xlog_tx_commit(l);
}

/**
 * Initialize WAL writer context. Even though it's a singleton,
 * encapsulate the details just in case we may use
 * more writers in the future.
 */
static void
wal_writer_create(struct wal_writer *writer, enum wal_mode wal_mode,
		  const char *wal_dirname, int64_t wal_max_rows,
		  int64_t wal_max_size, const struct tt_uuid *instance_uuid,
		  const struct vclock *vclock, int64_t checkpoint_lsn)
{
	writer->wal_mode = wal_mode;
	writer->wal_max_rows = wal_max_rows;
	writer->wal_max_size = wal_max_size;
	journal_create(&writer->base, wal_mode == WAL_NONE ?
		       wal_write_in_wal_mode_none : wal_write, NULL);

	xdir_create(&writer->wal_dir, wal_dirname, XLOG, instance_uuid);
	xlog_clear(&writer->current_wal);
	if (wal_mode == WAL_FSYNC)
		writer->wal_dir.open_wflags |= O_SYNC;

	stailq_create(&writer->queue);

	stailq_create(&writer->rollback);
	writer->in_rollback = false;

	/* Create and fill writer->vclock. */
	vclock_create(&writer->vclock);
	vclock_copy(&writer->vclock, vclock);
	vclock_create(&writer->commit_vclock);
	vclock_copy(&writer->commit_vclock, vclock);

	struct wal_buf_item *last;
	last = (struct wal_buf_item *)writer->wal_buf_index.wpos - 1;
	vclock_copy(&last->vclock, vclock);


	writer->checkpoint_lsn = checkpoint_lsn;
	rlist_create(&writer->watchers);
	writer->last_entry = NULL;
	fiber_cond_create(&writer->rollback_cond);
}

/** Destroy a WAL writer structure. */
static void
wal_writer_destroy(struct wal_writer *writer)
{
	xdir_destroy(&writer->wal_dir);
}

/** WAL thread routine. */
static int
wal_thread_f(va_list ap);

/** Start WAL thread and setup pipes to and from TX. */
void
wal_thread_start()
{
	if (cord_costart(&wal_thread.cord, "wal", wal_thread_f, NULL) != 0)
		panic("failed to start WAL thread");

	/* Create a pipe to WAL thread. */
	cpipe_create(&wal_thread.wal_pipe, "wal");
	cpipe_set_max_input(&wal_thread.wal_pipe, IOV_MAX);
}

static int
wal_open_f(struct cbus_call_msg *msg)
{
	(void)msg;
	struct wal_writer *writer = &wal_writer_singleton;
	//FIXME: do not use writer from wal thread
	const char *path = xdir_format_filename(&writer->wal_dir,
				vclock_sum(&writer->vclock), NONE);
	assert(!xlog_is_open(&writer->current_wal));
	return xlog_open(&writer->current_wal, path);
}

/**
 * Try to open the current WAL file for appending if it exists.
 */
static int
wal_open(struct wal_writer *writer)
{
	const char *path = xdir_format_filename(&writer->wal_dir,
				vclock_sum(&writer->vclock), NONE);
	if (access(path, F_OK) != 0) {
		if (errno == ENOENT) {
			/* No WAL, nothing to do. */
			return 0;
		}
		diag_set(SystemError, "failed to access %s", path);
		return -1;
	}

	/*
	 * The WAL file exists, try to open it.
	 *
	 * Note, an xlog object cannot be opened and used in
	 * different threads (because it uses slab arena), so
	 * we have to call xlog_open() on behalf of the WAL
	 * thread.
	 */
	struct cbus_call_msg msg;
	if (cbus_call(&wal_thread.wal_pipe, &wal_thread.tx_prio_pipe, &msg,
		      wal_open_f, NULL, TIMEOUT_INFINITY) == 0) {
		/*
		 * Success: we can now append to
		 * the existing WAL file.
		 */
		return 0;
	}
	struct error *e = diag_last_error(diag_get());
	if (!type_assignable(&type_XlogError, e->type)) {
		/*
		 * Out of memory or system error.
		 * Nothing we can do.
		 */
		return -1;
	}
	diag_log();

	/*
	 * Looks like the WAL file is corrupted.
	 * Rename it so that we can proceed.
	 */
	say_warn("renaming corrupted %s", path);
	char new_path[PATH_MAX];
	snprintf(new_path, sizeof(new_path), "%s.corrupted", path);
	if (rename(path, new_path) != 0) {
		diag_set(SystemError, "failed to rename %s", path);
		return -1;
	}
	return 0;
}

/**
 * Initialize WAL writer.
 *
 * @pre   The instance has completed recovery from a snapshot
 *	and/or existing WALs. All WALs opened in read-only
 *	mode are closed. WAL thread has been started.
 */
int
wal_init(enum wal_mode wal_mode, const char *wal_dirname, int64_t wal_max_rows,
	 int64_t wal_max_size, const struct tt_uuid *instance_uuid,
	 const struct vclock *vclock, int64_t first_checkpoint_lsn)
{
	assert(wal_max_rows > 1);

	struct wal_writer *writer = &wal_writer_singleton;
	wal_writer_create(writer, wal_mode, wal_dirname, wal_max_rows,
			  wal_max_size, instance_uuid, vclock,
			  first_checkpoint_lsn);

	/*
	 * Scan the WAL directory to build an index of all
	 * existing WAL files. Required for garbage collection,
	 * see wal_collect_garbage().
	 */
	if (xdir_scan(&writer->wal_dir))
		return -1;

	if (wal_open(writer) != 0)
		return -1;

	journal_set(&writer->base);
	return 0;
}

/**
 * Stop WAL thread, wait until it exits, and destroy WAL writer
 * if it was initialized. Called on shutdown.
 */
void
wal_thread_stop()
{
	cbus_stop_loop(&wal_thread.wal_pipe);

	if (cord_join(&wal_thread.cord)) {
		/* We can't recover from this in any reasonable way. */
		panic_syserror("WAL writer: thread join failed");
	}

	if (journal_is_initialized(&wal_writer_singleton.base))
		wal_writer_destroy(&wal_writer_singleton);
}

struct wal_checkpoint
{
	struct cmsg base;
	struct vclock *vclock;
	struct fiber *fiber;
	bool rotate;
	int res;
};

void
wal_checkpoint_f(struct cmsg *data)
{
	struct wal_checkpoint *msg = (struct wal_checkpoint *) data;
	struct wal_writer *writer = &wal_writer_singleton;
	if (writer->in_rollback) {
		/* We're rolling back a failed write. */
		msg->res = -1;
		return;
	}
	/*
	 * Avoid closing the current WAL if it has no rows (empty).
	 */
	if (msg->rotate && xlog_is_open(&writer->current_wal) &&
	    vclock_sum(&writer->current_wal.meta.vclock) !=
	    vclock_sum(&writer->vclock)) {

		xlog_close(&writer->current_wal, false);
		/*
		 * The next WAL will be created on the first write.
		 */
	}
	vclock_copy(msg->vclock, &writer->vclock);
}

void
wal_checkpoint_done_f(struct cmsg *data)
{
	struct wal_checkpoint *msg = (struct wal_checkpoint *) data;
	fiber_wakeup(msg->fiber);
}

int
wal_checkpoint(struct vclock *vclock, bool rotate)
{
	struct wal_writer *writer = &wal_writer_singleton;
	if (! stailq_empty(&writer->rollback)) {
		/*
		 * The writer rollback queue is not empty,
		 * roll back this transaction immediately.
		 * This is to ensure we do not accidentally
		 * commit a transaction which has seen changes
		 * that will be rolled back.
		 */
		say_error("Aborting transaction %llu during "
			  "cascading rollback",
			  vclock_sum(&writer->vclock));
		return -1;
	}
	if (writer->wal_mode == WAL_NONE) {
		vclock_copy(vclock, &writer->vclock);
		return 0;
	}
	static struct cmsg_hop wal_checkpoint_route[] = {
		{wal_checkpoint_f, &wal_thread.tx_prio_pipe},
		{wal_checkpoint_done_f, NULL},
	};
	vclock_create(vclock);
	struct wal_checkpoint msg;
	cmsg_init(&msg.base, wal_checkpoint_route);
	msg.vclock = vclock;
	msg.fiber = fiber();
	msg.rotate = rotate;
	msg.res = 0;
	cpipe_push(&wal_thread.wal_pipe, &msg.base);
	fiber_set_cancellable(false);
	fiber_yield();
	fiber_set_cancellable(true);
	return msg.res;
}

struct wal_gc_msg
{
	struct cbus_call_msg base;
	int64_t wal_lsn;
	int64_t checkpoint_lsn;
};

static int
wal_collect_garbage_f(struct cbus_call_msg *data)
{
	struct wal_writer *writer = &wal_writer_singleton;
	struct wal_gc_msg *msg = (struct wal_gc_msg *)data;
	writer->checkpoint_lsn = msg->checkpoint_lsn;
	xdir_collect_garbage(&writer->wal_dir, msg->wal_lsn, 0);
	return 0;
}

void
wal_collect_garbage(int64_t wal_lsn, int64_t checkpoint_lsn)
{
	struct wal_writer *writer = &wal_writer_singleton;
	if (writer->wal_mode == WAL_NONE)
		return;
	struct wal_gc_msg msg;
	msg.wal_lsn = wal_lsn;
	msg.checkpoint_lsn = checkpoint_lsn;
	bool cancellable = fiber_set_cancellable(false);
	cbus_call(&wal_thread.wal_pipe, &wal_thread.tx_prio_pipe, &msg.base,
		  wal_collect_garbage_f, NULL, TIMEOUT_INFINITY);
	fiber_set_cancellable(cancellable);
}

static void
wal_notify_watchers(struct wal_writer *writer, unsigned events);

/**
 * If there is no current WAL, try to open it, and close the
 * previous WAL. We close the previous WAL only after opening
 * a new one to smoothly move local hot standby and replication
 * over to the next WAL.
 * In case of error, we try to close any open WALs.
 *
 * @post r->current_wal is in a good shape for writes or is NULL.
 * @return 0 in case of success, -1 on error.
 */
static int
wal_opt_rotate(struct wal_writer *writer)
{
	ERROR_INJECT_RETURN(ERRINJ_WAL_ROTATE);

	/*
	 * Close the file *before* we create the new WAL, to
	 * make sure local hot standby/replication can see
	 * EOF in the old WAL before switching to the new
	 * one.
	 */
	if (xlog_is_open(&writer->current_wal) &&
	    (writer->current_wal.rows >= writer->wal_max_rows ||
	     writer->current_wal.offset >= writer->wal_max_size)) {
		/*
		 * We can not handle xlog_close()
		 * failure in any reasonable way.
		 * A warning is written to the error log.
		 */
		xlog_close(&writer->current_wal, false);
	}

	if (xlog_is_open(&writer->current_wal))
		return 0;

	if (xdir_create_xlog(&writer->wal_dir, &writer->current_wal,
			     &writer->vclock) != 0) {
		diag_log();
		return -1;
	}
	/*
	 * Keep track of the new WAL vclock. Required for garbage
	 * collection, see wal_collect_garbage().
	 */
	xdir_add_vclock(&writer->wal_dir, &writer->vclock);

	wal_notify_watchers(writer, WAL_EVENT_ROTATE);
	return 0;
}

/**
 * Make sure there's enough disk space to append @len bytes
 * of data to the current WAL.
 *
 * If fallocate() fails with ENOSPC, delete old WAL files
 * that are not needed for recovery and retry.
 */
static int
wal_fallocate(struct wal_writer *writer, size_t len)
{
	bool warn_no_space = true;
	struct xlog *l = &writer->current_wal;
	struct errinj *errinj = errinj(ERRINJ_WAL_FALLOCATE, ERRINJ_INT);

	/*
	 * The actual write size can be greater than the sum size
	 * of encoded rows (compression, fixheaders). Double the
	 * given length to get a rough upper bound estimate.
	 */
	len *= 2;

retry:
	if (errinj == NULL || errinj->iparam == 0) {
		if (l->allocated >= len)
			return 0;
		if (xlog_fallocate(l, MAX(len, WAL_FALLOCATE_LEN)) == 0)
			return 0;
	} else {
		errinj->iparam--;
		diag_set(ClientError, ER_INJECTION, "xlog fallocate");
		errno = ENOSPC;
	}
	if (errno != ENOSPC)
		goto error;
	if (!xdir_has_garbage(&writer->wal_dir, writer->checkpoint_lsn))
		goto error;

	if (warn_no_space) {
		say_crit("ran out of disk space, try to delete old WAL files");
		warn_no_space = false;
	}

	/* Keep the original error. */
	struct diag diag;
	diag_create(&diag);
	diag_move(diag_get(), &diag);
	if (xdir_collect_garbage(&writer->wal_dir, writer->checkpoint_lsn,
				 XDIR_GC_REMOVE_ONE) != 0) {
		diag_move(&diag, diag_get());
		goto error;
	}
	diag_destroy(&diag);

	wal_notify_watchers(writer, WAL_EVENT_GC);
	goto retry;
error:
	diag_log();
	return -1;
}

static void
wal_assign_lsn(struct xrow_header **row,
	       struct xrow_header **end, struct vclock *vclock)
{
	/** Assign LSN to all local rows. */
	for ( ; row < end; row++) {
		if ((*row)->replica_id == 0) {
			(*row)->lsn = vclock_inc(vclock, instance_id);
			(*row)->replica_id = instance_id;
		} else {
			vclock_follow_xrow(vclock, *row);
		}
	}
}

static int
wal_mem_rotate(struct wal_writer *writer)
{
	struct wal_buf_item *last;
	last = (struct wal_buf_item *)writer->wal_buf_index.wpos - 1;
	uint8_t buf_no = last->buf_no;
	if (ibuf_used(&writer->wal_buf[buf_no]) > 8 * 1024 * 1024) {
		struct wal_buf_item *first;
		first = (struct wal_buf_item *)writer->wal_buf_index.rpos;
		while (first->buf_no == 1 - buf_no)
			++first;
		writer->wal_buf_index.rpos = (char *)first;

		buf_no = 1 - buf_no;
		ibuf_reset(&writer->wal_buf[buf_no]);

		if (last->size > 0)
			last = (struct wal_buf_item *)ibuf_alloc(&writer->wal_buf_index,
							 sizeof(struct wal_buf_item));
		if (last == NULL)
			return -1;
		last->buf_no = buf_no;
		last->size = 0;
		last->pos = 0;
		vclock_copy(&last->vclock, &writer->vclock);
	}
	return writer->wal_buf[buf_no].wpos - writer->wal_buf[buf_no].buf;
}

static int
wal_relay_broadcast(struct wal_writer *writer, int data_pos)
{
	struct wal_relay *wal_relay;
	struct wal_buf_item *last;
	last = (struct wal_buf_item *)writer->wal_buf_index.wpos - 1;
	struct ibuf *mem_buf = writer->wal_buf + last->buf_no;
	const char *data = mem_buf->buf + data_pos;
	ssize_t to_write = mem_buf->wpos - data;
	rlist_foreach_entry(wal_relay, &writer->relay, item){
		if (wal_relay->state != WAL_RELAY_ONLINE)
			continue;
		int written = write(wal_relay->io.fd, data, to_write);
		if (written == to_write) {
			vclock_copy(&wal_relay->send_vclock, &writer->vclock);
			continue;
		}
		if (written > 0) {
			wal_relay->state = WAL_RELAY_MEM;
			vclock_copy(&wal_relay->send_vclock, &writer->vclock);
			wal_relay->to_send = to_write - written;
			wal_relay->send_buf = region_alloc(&wal_relay->fiber->gc,
							   wal_relay->to_send);
			if (wal_relay->send_buf != NULL)
				memcpy(wal_relay->send_buf, data + written,
				       wal_relay->to_send);
			else
				wal_relay->state = WAL_RELAY_ERROR;
		} else {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				wal_relay->state = WAL_RELAY_MEM;
			else
				wal_relay->state = WAL_RELAY_ERROR;
		}
		fiber_cond_signal(&wal_relay->online);
	}
	return mem_buf->wpos - mem_buf->buf;
}

static ssize_t
wal_encode_entry(struct wal_writer *writer, struct journal_entry *entry,
		 struct vclock *req_vclock)
{
	struct wal_buf_item *last;
	last = (struct wal_buf_item *)writer->wal_buf_index.wpos - 1;
	int buf_no = last->buf_no;
	struct ibuf *mem_buf = writer->wal_buf + buf_no;

	if (last->size > 16384) {
		last = (struct wal_buf_item *)
			ibuf_alloc(&writer->wal_buf_index, sizeof(struct wal_buf_item));
		if (last == NULL)
			return -1;
		last->size = 0;
		last->buf_no = buf_no;
		last->pos = ibuf_used(mem_buf);
		vclock_copy(&last->vclock, req_vclock);
	}

	wal_assign_lsn(entry->rows, entry->rows + entry->n_rows, req_vclock);

	entry->res = vclock_sum(req_vclock);

	struct xrow_header **row = entry->rows;

	if (entry->rows[0]->type == IPROTO_COMMIT) {
	int64_t txn = entry->rows[0]->lsn;
	for (; row < entry->rows + entry->n_rows; row++) {
		(*row)->tm = ev_now(loop());
		if (row < entry->rows + entry->n_rows - 1)
			(*row)->txn = txn;
		else
			(*row)->txn = 0;
	}}

	uint64_t old_size = last->size;

	row = entry->rows;
	while (row < entry->rows + entry->n_rows) {
		struct iovec iov[XROW_IOVMAX];
		int iovcnt = xrow_to_iovec(*row, iov);
		if (iovcnt < 0)
			goto error;
		uint64_t xrow_size = 0;
		for (int i = 0; i < iovcnt; ++i)
			xrow_size += iov[i].iov_len;
		if (ibuf_reserve(mem_buf, xrow_size) == NULL)
			goto error;

		for (int i = 0; i < iovcnt; ++i) {
			memcpy(ibuf_alloc(mem_buf, iov[i].iov_len),
			       iov[i].iov_base, iov[i].iov_len);
			last->size += iov[i].iov_len;
		}
		++row;
	}
	return last->size - old_size;

error:
	last->size = old_size;
	mem_buf->wpos = mem_buf->buf + last->pos + last->size;
	return -1;
}

static int
wal_promote(struct xrow_header *row)
{
	struct wal_writer *writer = &wal_writer_singleton;
	struct vclock req_vclock;
	vclock_create(&req_vclock);
	vclock_copy(&req_vclock, &writer->vclock);
	struct journal_entry *entry = malloc(sizeof(struct journal_entry) +
					     sizeof(struct xrow_header *));

	entry->rows[0] = row;
	entry->n_rows = 1;
	struct xlog *l = &writer->current_wal;

	int dirty_pos = wal_mem_rotate(writer);
	int rc = wal_encode_entry(writer, entry, &req_vclock);
	if (rc < 0)
		goto error;

	rc = xlog_write_entry(l, entry);
	if (rc < 0)
		goto error;

	if (xlog_flush(l) < 0)
		goto error;
	wal_relay_broadcast(writer, dirty_pos);

	vclock_copy(&writer->vclock, &req_vclock);
	vclock_follow(&writer->commit_vclock, row->txn_replica_id, row->txn);
	fiber_cond_signal(&writer->commit_cond);

	free(entry);
	return row->lsn;

error:
	free(entry);
	return -1;
}

static void
wal_write_to_disk(struct cmsg *msg)
{
	struct wal_writer *writer = &wal_writer_singleton;
	struct wal_msg *wal_msg = (struct wal_msg *) msg;
	struct error *error;

	struct vclock req_vclock;
	vclock_create(&req_vclock);
	vclock_copy(&req_vclock, &writer->vclock);

	struct errinj *inj = errinj(ERRINJ_WAL_DELAY, ERRINJ_BOOL);
	while (inj != NULL && inj->bparam)
		usleep(10);
	struct journal_entry *entry;

	if (writer->in_rollback ||	/* We're rolling back a failed write. */
	   wal_opt_rotate(writer) != 0 ||	/* Xlog is only rotated between queue processing  */
	   wal_fallocate(writer, wal_msg->approx_len) != 0)	/* Ensure there's enough disk space before writing anything. */
	{
		wal_queue_rollback(&wal_msg->commit);
		return;
	}

	/*
	 * This code tries to write queued requests (=transactions) using as
	 * few I/O syscalls and memory copies as possible. For this reason
	 * writev(2) and `struct iovec[]` are used (see `struct fio_batch`).
	 *
	 * For each request (=transaction) each request row (=statement) is
	 * added to iov `batch`. A row can contain up to XLOG_IOVMAX iovecs.
	 * A request can have an **unlimited** number of rows. Since OS has
	 * a hard coded limit up to `sysconf(_SC_IOV_MAX)` iovecs (usually
	 * 1024), a huge transaction may not fit into a single batch.
	 * Therefore, it is not possible to "atomically" write an entire
	 * transaction using a single writev(2) call.
	 *
	 * Request boundaries and batch boundaries are not connected at all
	 * in this code. Batches flushed to disk as soon as they are full.
	 * In order to guarantee that a transaction is either fully written
	 * to file or isn't written at all, ftruncate(2) is used to shrink
	 * the file to the last fully written request. The absolute position
	 * of request in xlog file is stored inside `struct journal_entry`.
	 */

	struct xlog *l = &writer->current_wal;

	/*
	 * Iterate over requests (transactions)
	 */

	int dirty_pos = wal_mem_rotate(writer);

	struct stailq_entry *last_written = NULL;
	stailq_foreach_entry(entry, &wal_msg->commit, fifo) {
	//FIXME: roolback memory if error
		int rc = wal_encode_entry(writer, entry, &req_vclock);
		if (rc < 0)
			goto done;
		rc = xlog_write_entry(l, entry);
		if (rc < 0)
			goto done;

		/* rc == 0: the write is buffered in xlog_tx */
		if (rc == 0)
			continue;

		last_written = &entry->fifo;
		vclock_copy(&writer->vclock, &req_vclock);
		dirty_pos = wal_relay_broadcast(writer, dirty_pos);
	}
	if (xlog_flush(l) < 0)
		goto done;

	last_written = stailq_last(&wal_msg->commit);
	vclock_copy(&writer->vclock, &req_vclock);
	dirty_pos = wal_relay_broadcast(writer, dirty_pos);

done:
	error = diag_last_error(diag_get());
	if (error) {
		/* Until we can pass the error to tx, log it and clear. */
		error_log(error);
		diag_clear(diag_get());
	}
	if (rlist_empty(&writer->relay) &&
	    vclock_get(&writer->vclock, instance_id) >
	    vclock_get(&writer->commit_vclock, instance_id))
		vclock_follow(&writer->commit_vclock, instance_id,
			      vclock_get(&writer->vclock, instance_id));
	/*
	 * We need to start rollback from the first request
	 * following the last committed request. If
	 * last_commit_req is NULL, it means we have committed
	 * nothing, and need to start rollback from the first
	 * request. Otherwise we rollback from the first request.
	 */
	struct stailq rollback;
	stailq_cut_tail(&wal_msg->commit, last_written, &rollback);

	stailq_concat(&writer->queue, &wal_msg->commit);

	if (!stailq_empty(&rollback)) {
		wal_queue_rollback(&rollback);
	}
	fiber_gc();
	wal_notify_watchers(writer, WAL_EVENT_WRITE);
	fiber_cond_signal(&writer->status_cond);
	fiber_cond_signal(&writer->commit_cond);
}

/** WAL thread main loop.  */
static int
wal_thread_f(va_list ap)
{
	(void) ap;
	struct wal_writer *writer = &wal_writer_singleton;

	/** Initialize eio in this thread */
	coio_enable();

	struct cbus_endpoint endpoint;
	cbus_endpoint_create(&endpoint, "wal", fiber_schedule_cb, fiber());
	/*
	 * Create a pipe to TX thread. Use a high priority
	 * endpoint, to ensure that WAL messages are delivered
	 * even when tx fiber pool is used up by net messages.
	 */
	cpipe_create(&wal_thread.tx_prio_pipe, "tx_prio");

	ibuf_create(&writer->wal_buf_index, &cord()->slabc, 8192);
	struct wal_buf_item *ind;
	ind = ibuf_alloc(&writer->wal_buf_index, sizeof(struct wal_buf_item));
	vclock_copy(&ind->vclock, &writer->vclock);
	ind->pos = 0;
	ind->size = 0;
	ind->buf_no = 0;
	ibuf_create(&writer->wal_buf[0], &cord()->slabc, 65536);
	ibuf_create(&writer->wal_buf[1], &cord()->slabc, 65536);
	rlist_create(&writer->relay);

	fiber_cond_create(&writer->commit_cond);
	writer->commit_fiber = fiber_new("commit", wal_commit_f);
	fiber_start(writer->commit_fiber, NULL);

	fiber_cond_create(&writer->status_cond);
	rlist_create(&writer->on_wal_status);
	writer->status_fiber = fiber_new("status", wal_status_f);
	fiber_start(writer->status_fiber, NULL);

	cbus_loop(&endpoint);


	/*
	 * Create a new empty WAL on shutdown so that we don't
	 * have to rescan the last WAL to find the instance vclock.
	 * Don't create a WAL if the last one is empty.
	 */
	if (writer->wal_mode != WAL_NONE &&
	    (!xlog_is_open(&writer->current_wal) ||
	     vclock_compare(&writer->vclock,
			    &writer->current_wal.meta.vclock) > 0)) {
		struct xlog l;
		if (xdir_create_xlog(&writer->wal_dir, &l,
				     &writer->vclock) == 0)
			xlog_close(&l, false);
		else
			diag_log();
	}

	if (xlog_is_open(&writer->current_wal))
		xlog_close(&writer->current_wal, false);

	if (xlog_is_open(&vy_log_writer.xlog))
		xlog_close(&vy_log_writer.xlog, false);

	cpipe_destroy(&wal_thread.tx_prio_pipe);
	return 0;
}

/**
 * WAL writer main entry point: queue a single request
 * to be written to disk and wait until this task is completed.
 */
int64_t
wal_write(struct journal *journal, struct journal_entry *entry)
{
	struct wal_writer *writer = (struct wal_writer *) journal;

	ERROR_INJECT_RETURN(ERRINJ_WAL_IO);

	if (! stailq_empty(&writer->rollback)) {
		/*
		 * The writer rollback queue is not empty,
		 * roll back this transaction immediately.
		 * This is to ensure we do not accidentally
		 * commit a transaction which has seen changes
		 * that will be rolled back.
		 */
		say_error("Aborting transaction %llu during "
			  "cascading rollback",
			  vclock_sum(&writer->vclock));
		return -1;
	}

	struct wal_msg *batch;
	if (!stailq_empty(&wal_thread.wal_pipe.input) &&
	    (batch = wal_msg(stailq_first_entry(&wal_thread.wal_pipe.input,
						struct cmsg, fifo)))) {

		stailq_add_tail_entry(&batch->commit, entry, fifo);
	} else {
		batch = (struct wal_msg *)
			region_alloc(&fiber()->gc, sizeof(struct wal_msg));
		if (batch == NULL) {
			diag_set(OutOfMemory, sizeof(struct wal_msg),
				 "region", "struct wal_msg");
			return -1;
		}
		wal_msg_create(batch);
		/*
		 * Sic: first add a request, then push the batch,
		 * since cpipe_push() may pass the batch to WAL
		 * thread right away.
		 */
		stailq_add_tail_entry(&batch->commit, entry, fifo);
		cpipe_push(&wal_thread.wal_pipe, &batch->base);
	}
	writer->last_entry = entry;
	batch->approx_len += entry->approx_len;
	wal_thread.wal_pipe.n_input += entry->n_rows * XROW_IOVMAX;
	cpipe_flush_input(&wal_thread.wal_pipe);
	/**
	 * It's not safe to spuriously wakeup this fiber
	 * since in that case it will ignore a possible
	 * error from WAL writer and not roll back the
	 * transaction.
	 */
	bool cancellable = fiber_set_cancellable(false);

	say_error("send wal");

	fiber_yield(); /* Request was inserted. */
	say_error("recv wal");
	fiber_set_cancellable(cancellable);
	if (entry->res < 0) {
		while (stailq_first_entry(&writer->rollback,
					  struct journal_entry, fifo) != entry)
			fiber_cond_wait(&writer->rollback_cond);
		stailq_shift(&writer->rollback);
		fiber_cond_broadcast(&writer->rollback_cond);
	}
	return entry->res;
}

int64_t
wal_write_in_wal_mode_none(struct journal *journal,
			   struct journal_entry *entry)
{
	struct wal_writer *writer = (struct wal_writer *) journal;
	wal_assign_lsn(entry->rows, entry->rows + entry->n_rows,
		       &writer->vclock);
	int64_t old_lsn = vclock_get(&replicaset.vclock, instance_id);
	int64_t new_lsn = vclock_get(&writer->vclock, instance_id);
	if (new_lsn > old_lsn) {
		/* There were local writes, promote vclock. */
		vclock_follow(&replicaset.vclock, instance_id, new_lsn);
	}
	return vclock_sum(&writer->vclock);
}

void
wal_init_vy_log()
{
	xlog_clear(&vy_log_writer.xlog);
}

struct wal_write_vy_log_msg
{
	struct cbus_call_msg base;
	struct journal_entry *entry;
};

static int
wal_write_vy_log_f(struct cbus_call_msg *msg)
{
	struct journal_entry *entry =
		((struct wal_write_vy_log_msg *)msg)->entry;

	if (! xlog_is_open(&vy_log_writer.xlog)) {
		if (vy_log_open(&vy_log_writer.xlog) < 0)
			return -1;
	}

	if (xlog_write_entry(&vy_log_writer.xlog, entry) < 0)
		return -1;

	if (xlog_flush(&vy_log_writer.xlog) < 0)
		return -1;

	return 0;
}

int
wal_write_vy_log(struct journal_entry *entry)
{
	struct wal_write_vy_log_msg msg;
	msg.entry= entry;
	bool cancellable = fiber_set_cancellable(false);
	int rc = cbus_call(&wal_thread.wal_pipe, &wal_thread.tx_prio_pipe,
			   &msg.base, wal_write_vy_log_f, NULL,
			   TIMEOUT_INFINITY);
	fiber_set_cancellable(cancellable);
	return rc;
}

static int
wal_rotate_vy_log_f(struct cbus_call_msg *msg)
{
	(void) msg;
	if (xlog_is_open(&vy_log_writer.xlog))
		xlog_close(&vy_log_writer.xlog, false);
	return 0;
}

void
wal_rotate_vy_log()
{
	struct cbus_call_msg msg;
	bool cancellable = fiber_set_cancellable(false);
	cbus_call(&wal_thread.wal_pipe, &wal_thread.tx_prio_pipe, &msg,
		  wal_rotate_vy_log_f, NULL, TIMEOUT_INFINITY);
	fiber_set_cancellable(cancellable);
}

static void
wal_watcher_notify(struct wal_watcher *watcher, unsigned events)
{
	assert(!rlist_empty(&watcher->next));

	struct wal_watcher_msg *msg = &watcher->msg;
	struct wal_writer *writer = &wal_writer_singleton;

	events &= watcher->event_mask;
	if (events == 0) {
		/* The watcher isn't interested in this event. */
		return;
	}

	if (msg->cmsg.route != NULL) {
		/*
		 * If the notification message is still en route,
		 * mark the watcher to resend it as soon as it
		 * returns to WAL so as not to lose any events.
		 */
		watcher->pending_events |= events;
		return;
	}

	msg->events = events;
	if (xdir_first_vclock(&writer->wal_dir, &msg->gc_vclock) < 0)
		vclock_copy(&msg->gc_vclock, &writer->vclock);

	cmsg_init(&msg->cmsg, watcher->route);
	cpipe_push(&watcher->watcher_pipe, &msg->cmsg);
}

static void
wal_watcher_notify_perform(struct cmsg *cmsg)
{
	struct wal_watcher_msg *msg = (struct wal_watcher_msg *) cmsg;
	msg->watcher->cb(msg);
}

static void
wal_watcher_notify_complete(struct cmsg *cmsg)
{
	struct wal_watcher_msg *msg = (struct wal_watcher_msg *) cmsg;
	struct wal_watcher *watcher = msg->watcher;

	cmsg->route = NULL;

	if (rlist_empty(&watcher->next)) {
		/* The watcher is about to be destroyed. */
		return;
	}

	if (watcher->pending_events != 0) {
		/*
		 * Resend the message if we got notified while
		 * it was en route, see wal_watcher_notify().
		 */
		wal_watcher_notify(watcher, watcher->pending_events);
		watcher->pending_events = 0;
	}
}

static void
wal_watcher_attach(void *arg)
{
	struct wal_watcher *watcher = (struct wal_watcher *) arg;
	struct wal_writer *writer = &wal_writer_singleton;

	assert(rlist_empty(&watcher->next));
	rlist_add_tail_entry(&writer->watchers, watcher, next);

	/*
	 * Notify the watcher right after registering it
	 * so that it can process existing WALs.
	 */
	wal_watcher_notify(watcher, WAL_EVENT_ROTATE);
}

static void
wal_watcher_detach(void *arg)
{
	struct wal_watcher *watcher = (struct wal_watcher *) arg;

	assert(!rlist_empty(&watcher->next));
	rlist_del_entry(watcher, next);
}

void
wal_set_watcher(struct wal_watcher *watcher, const char *name,
		void (*watcher_cb)(struct wal_watcher_msg *),
		void (*process_cb)(struct cbus_endpoint *),
		unsigned event_mask)
{
	assert(journal_is_initialized(&wal_writer_singleton.base));

	rlist_create(&watcher->next);
	watcher->cb = watcher_cb;
	watcher->msg.watcher = watcher;
	watcher->msg.events = 0;
	watcher->msg.cmsg.route = NULL;
	watcher->pending_events = 0;
	watcher->event_mask = event_mask;

	assert(lengthof(watcher->route) == 2);
	watcher->route[0] = (struct cmsg_hop)
		{ wal_watcher_notify_perform, &watcher->wal_pipe };
	watcher->route[1] = (struct cmsg_hop)
		{ wal_watcher_notify_complete, NULL };
	cbus_pair("wal", name, &watcher->wal_pipe, &watcher->watcher_pipe,
		  wal_watcher_attach, watcher, process_cb);
}

void
wal_clear_watcher(struct wal_watcher *watcher,
		  void (*process_cb)(struct cbus_endpoint *))
{
	assert(journal_is_initialized(&wal_writer_singleton.base));

	cbus_unpair(&watcher->wal_pipe, &watcher->watcher_pipe,
		    wal_watcher_detach, watcher, process_cb);
}

/**
 * Notify all interested watchers about a WAL event.
 *
 * XXX: Note, this function iterates over all registered watchers,
 * including those that are not interested in the given event.
 * This is OK only as long as the number of events/watchers is
 * small. If this ever changes, we should consider maintaining
 * a separate watcher list per each event type.
 */
static void
wal_notify_watchers(struct wal_writer *writer, unsigned events)
{
	struct wal_watcher *watcher;
	rlist_foreach_entry(watcher, &writer->watchers, next)
		wal_watcher_notify(watcher, events);
}


/**
 * After fork, the WAL writer thread disappears.
 * Make sure that atexit() handlers in the child do
 * not try to stop a non-existent thread or write
 * a second EOF marker to an open file.
 */
void
wal_atfork()
{
	if (xlog_is_open(&wal_writer_singleton.current_wal))
		xlog_atfork(&wal_writer_singleton.current_wal);
	if (xlog_is_open(&vy_log_writer.xlog))
		xlog_atfork(&vy_log_writer.xlog);
}

struct wal_relay_msg {
	struct cmsg base;
	struct replica *replica;
	struct vclock vclock;
	struct ev_io *io;
	uint64_t sync;
};

static void
wal_relay_start(struct cmsg *msg);

void
wal_relay(struct replica *replica, struct ev_io *io, uint64_t sync, struct vclock *vclock)
{
	static struct cmsg_hop wal_relay_start_route[1] = {
		{wal_relay_start, NULL}
	};

	struct wal_relay_msg *msg;
	msg = (struct wal_relay_msg *)malloc(sizeof(struct wal_relay_msg));

	msg->replica = replica;
	vclock_copy(&msg->vclock, vclock);
	msg->io = io;
	msg->sync = sync;
	cmsg_init(&msg->base, wal_relay_start_route);
	cpipe_push(&wal_thread.wal_pipe, &msg->base);
	fiber_yield();
}

/**
 * Cbus message to send status updates from relay to tx thread.
 */
struct relay_status_msg {
	/** Parent */
	struct cmsg msg;
	/** Relay instance */
	struct wal_relay *relay;
	/** Replica vclock. */
	struct vclock vclock;
};


static int
wal_relay_status_f(va_list ap)
{
	struct wal_writer *writer = &wal_writer_singleton;
	struct wal_relay *relay = va_arg(ap, struct wal_relay *);

	struct ibuf ibuf;
	ibuf_create(&ibuf, &cord()->slabc, 1024);
	while (!fiber_is_cancelled()) {
		struct xrow_header xrow;
		coio_read_xrow_timeout_xc(&relay->io, &ibuf, &xrow, 3600);
		/* vclock is followed while decoding, zeroing it. */
		vclock_create(&relay->recv_vclock);
		xrow_decode_vclock(&xrow, &relay->recv_vclock);
		struct rlist *target = &relay->item;
		while (rlist_next(target) != &writer->relay) {
			struct rlist *next = rlist_next(target);
			struct wal_relay *next_relay = container_of(next, struct wal_relay, item);
			if (vclock_get(&relay->recv_vclock, instance_id) <
			    vclock_get(&next_relay->recv_vclock, instance_id))
				break;
			target = next;
		}
		if (target != &relay->item)
			rlist_move(target, &relay->item);

		struct wal_relay *first_relay;
		first_relay = rlist_first_entry(&writer->relay,
						struct wal_relay,
						item);
		if (vclock_get(&first_relay->recv_vclock, instance_id) >
		    vclock_get(&writer->commit_vclock, instance_id)) {
			struct xrow_header row;
			row.type = IPROTO_COMMIT;
			row.lsn = 0;
			row.replica_id = 0;
			row.txn = vclock_get(&first_relay->recv_vclock, instance_id);
			row.txn_replica_id = instance_id;
			row.tm = ev_now(loop());
			row.bodycnt = 0;
			wal_promote(&row);
		}
		say_error("set commit %i", vclock_sum(&writer->commit_vclock));
		fiber_cond_signal(&writer->commit_cond);
	}
	return 0;
}

static int
wal_relay_send_heartbeat(struct wal_relay *relay)
{
	struct xrow_header row;
	xrow_encode_timestamp(&row, instance_id, ev_now(loop()));
	coio_write_xrow(&relay->io, &row);
	return 0;
}

struct relay_stream {
	struct xstream xstream;
	struct ibuf send_buf;
	struct wal_relay *wal_relay;
};

/** Send a single row to the client. */
static void
relay_send_row(struct xstream *stream, struct xrow_header *packet)
{
	struct relay_stream *relay_stream = container_of(stream,
							 struct relay_stream,
							 xstream);
	struct ibuf *send_buf = &relay_stream->send_buf;
	struct wal_relay *wal_relay = relay_stream->wal_relay;
	/*
	 * Transform replica local requests to IPROTO_NOP so as to
	 * promote vclock on the replica without actually modifying
	 * any data.
	 */
	//FIXME: client side transform
/*	if (packet->group_id == GROUP_LOCAL) {
		packet->type = IPROTO_NOP;
		packet->group_id = GROUP_DEFAULT;
		packet->bodycnt = 0;
	}*/
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
	if (true) {
		struct errinj *inj = errinj(ERRINJ_RELAY_BREAK_LSN,
					    ERRINJ_INT);
		if (inj != NULL && packet->lsn == inj->iparam) {
			packet->lsn = inj->iparam - 1;
			say_warn("injected broken lsn: %lld",
				 (long long) packet->lsn);
		}
	//	packet->sync = relay->sync;
	//	relay->last_row_tm = ev_monotonic_now(loop());
		struct iovec iov[XROW_IOVMAX];
		int iovcnt = xrow_to_iovec(packet, iov);
		int i;
		for (i = 0; i < iovcnt; ++i) {
			void *p = ibuf_alloc(send_buf, iov[i].iov_len);
			memcpy(p, iov[i].iov_base, iov[i].iov_len);
		}
		if (packet->txn == 0 && ibuf_used(send_buf) >= 64 * 1024) {
			write(wal_relay->io.fd, send_buf->rpos,
				   ibuf_used(send_buf));
			ibuf_reset(send_buf);
		}

	}
}

static int
relay_recovery_file(va_list ap)
{
	struct wal_writer *writer = &wal_writer_singleton;
	struct wal_relay *relay = va_arg(ap, struct wal_relay *);

	struct recovery *recovery;
	recovery = recovery_new(writer->wal_dir.dirname,
				writer->wal_dir.force_recovery,
				&relay->send_vclock);
	if (recovery == NULL)
		return -1;


	struct relay_stream relay_stream;
	xstream_create(&relay_stream.xstream, relay_send_row);
	ibuf_create(&relay_stream.send_buf, &cord()->slabc, 256 * 1024);
	relay_stream.wal_relay = relay;

	recover_remaining_wals(recovery, &relay_stream.xstream, NULL, true);
	write(relay->io.fd, relay_stream.send_buf.rpos,
	      ibuf_used(&relay_stream.send_buf));

	vclock_copy(&relay->send_vclock, &recovery->vclock);

	recovery_delete(recovery);
	return 0;
}

static int
relay_f(va_list ap)
{
	struct wal_writer *writer = &wal_writer_singleton;

	struct wal_relay_msg *msg = va_arg(ap, struct wal_relay_msg *);
	struct wal_relay relay;

	relay.replica = msg->replica;
	vclock_copy(&relay.send_vclock, &msg->vclock);
	vclock_copy(&relay.recv_vclock, &msg->vclock);
	//coio_create is a c++
	relay.io.data = fiber();
	ev_init(&relay.io, (ev_io_cb) fiber_schedule_cb);
	relay.io.fd = msg->io->fd;

	relay.state = WAL_RELAY_FILE;
	fiber_cond_create(&relay.online);
	relay.send_buf = NULL;
	relay.to_send = 0;
	relay.fiber = fiber();

	struct rlist *target = &writer->relay;
	while (rlist_next(target) != &writer->relay) {
		struct rlist *next = rlist_next(target);
		struct wal_relay *relay = container_of(next, struct wal_relay, item);
		if (vclock_get(&msg->vclock, instance_id) <
		    vclock_get(&relay->recv_vclock, instance_id))
			break;
		target = next;
	}
	rlist_add(target, &relay.item);

	char name[FIBER_NAME_MAX];
	snprintf(name, sizeof(name), "%s:%s", fiber()->name, "reader");
	struct fiber *reader = fiber_new(name, wal_relay_status_f);
	fiber_set_joinable(reader, true);
	fiber_start(reader, &relay);

	while (!fiber_is_cancelled() && relay.state != WAL_RELAY_ERROR) {
		struct wal_buf_item *first;
		first = (struct wal_buf_item *)writer->wal_buf_index.rpos;
		int cmp = vclock_compare(&relay.send_vclock, &first->vclock);
		if (cmp != 1 && cmp != 0) {
			relay.state = WAL_RELAY_FILE;
			struct cord cord;
			cord_costart(&cord, "file follow",
				     relay_recovery_file, &relay);
			cord_cojoin(&cord);
			continue;
		}
		relay.state = WAL_RELAY_MEM;
		struct wal_buf_item *last;
		last = (struct wal_buf_item *)writer->wal_buf_index.wpos - 1;
		struct wal_buf_item *mid = first;
		while (last - first > 1) {
			mid = first + (last - first) / 2;
			if (vclock_compare(&relay.send_vclock, &mid->vclock) != 1)
				mid = last;
			else
				mid = first;
		}
		last = (struct wal_buf_item *)writer->wal_buf_index.wpos - 1;
		while (mid <= last) {
			ssize_t written;
			struct ibuf *mem_buf = writer->wal_buf + mid->buf_no;
			written = write(relay.io.fd, mem_buf->buf + mid->pos,
					mid->size);
			if (written < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK)
					break;
				relay.state = WAL_RELAY_ERROR;
				goto error;
			}
			if (mid < last)
				vclock_copy(&relay.send_vclock,
					    &(mid + 1)->vclock);
			else
				vclock_copy(&relay.send_vclock,
					    &writer->vclock);

			if (written < (ssize_t)mid->size) {
				relay.to_send = mid->size - written;
				relay.send_buf = region_alloc(&fiber()->gc,
							      relay.to_send);
				if (relay.send_buf == NULL) {
					relay.state = WAL_RELAY_ERROR;
					goto error;
				}
				memcpy(relay.send_buf,
				       writer->wal_buf[mid->buf_no].rpos + written,
				       relay.to_send);
				break;
			}
			++mid;
		}
		if (mid > last)
			relay.state = WAL_RELAY_ONLINE;
		while (relay.state == WAL_RELAY_ONLINE) {
			double timeout = replication_timeout;
			struct errinj *inj = errinj(ERRINJ_RELAY_REPORT_INTERVAL,
						    ERRINJ_DOUBLE);
			if (inj != NULL && inj->dparam != 0)
				timeout = inj->dparam;
			//FIXME do not send from wal if sending
			if (fiber_cond_wait_timeout(&relay.online, timeout) < 0)
				wal_relay_send_heartbeat(&relay);
		}
		if (relay.state == WAL_RELAY_ERROR)
			goto error;
		while (coio_wait(relay.io.fd, COIO_WRITE, TIMEOUT_INFINITY) > 0 &&
		       relay.to_send > 0) {
			ssize_t written = write(relay.io.fd, relay.send_buf,
						relay.to_send);
			if (written < 0) {
				if (errno == EAGAIN || errno == EWOULDBLOCK) {
					relay.state = WAL_RELAY_ERROR;
					break;
				}
			}
			relay.to_send -= written;
			relay.send_buf += written;
			if (relay.to_send == 0)
				break;
		}
		fiber_gc();

	}
error:

	rlist_del(&relay.item);
	fiber_cancel(reader);
	fiber_join(reader);
	return 0;
}


static void
wal_relay_start(struct cmsg *base)
{
	struct wal_writer *wal_writer = &wal_writer_singleton;
	struct wal_relay_msg *msg;
	msg = container_of(base, struct wal_relay_msg, base);

	/*
	 * Send a response to SUBSCRIBE request, tell
	 * the replica how many rows we have in stock for it,
	 * and identify ourselves with our own replica id.
	 */
	struct xrow_header row;
	xrow_encode_vclock(&row, &wal_writer->vclock);
	/*
	 * Identify the message with the replica id of this
	 * instance, this is the only way for a replica to find
	 * out the id of the instance it has connected to.
	 */
	row.replica_id = instance_id;
	row.sync = msg->sync;
	coio_write_xrow(msg->io, &row);

	struct fiber *relay = fiber_new("relay", relay_f);
	fiber_start(relay, msg);
	free(msg);
}

struct wal_commit_msg {
	struct cmsg m;
	struct xrow_header *row;
	int64_t lsn;
	struct fiber_cond cond;
};

static void
wal_commit_do(struct cmsg *m)
{
	struct wal_commit_msg *msg = container_of(m, struct wal_commit_msg, m);
	msg->lsn = wal_promote(msg->row);
}

static void
wal_commit_done(struct cmsg *m)
{
	struct wal_commit_msg *msg = container_of(m, struct wal_commit_msg, m);
	fiber_cond_signal(&msg->cond);
}

struct cmsg_hop wal_commit_route[] = {
	{wal_commit_do, &wal_thread.tx_prio_pipe},
	{wal_commit_done, NULL}
};

int64_t
wal_commit(struct xrow_header *row)
{
	struct wal_commit_msg msg;
	msg.row = row;
	fiber_cond_create(&msg.cond);
	cmsg_init(&msg.m, wal_commit_route);
	cpipe_push(&wal_thread.wal_pipe, &msg.m);
	fiber_cond_wait(&msg.cond);
	return msg.lsn;
}

