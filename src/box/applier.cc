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
#include "applier.h"

#include <msgpuck.h>
#include <crc32.h>

#include "xlog.h"
#include "fiber.h"
#include "fiber_cond.h"
#include "coio.h"
#include "coio_buf.h"
#include "xstream.h"
#include "wal.h"
#include "xrow.h"
#include "replication.h"
#include "iproto_constants.h"
#include "version.h"
#include "trigger.h"
#include "xrow_io.h"
#include "error.h"
#include "session.h"
#include "cfg.h"
#include "schema.h"
#include "box.h"

STRS(applier_state, applier_STATE);

static int
replicaset_mapping_compare_by_uuid(const struct replicaset_mapping *a,
			   const struct replicaset_mapping *b)
{
	return tt_uuid_compare(&a->replica_uuid, &b->replica_uuid);
}

rb_gen(MAYBE_UNUSED static, replicaset_cache_, replicaset_cache_t,
       struct replicaset_mapping, link, replicaset_mapping_compare_by_uuid);


/** Cache of replicasets mapping. */
static replicaset_cache_t replicas_cache;
/** Counter of global uniting replica_ids. */
static  uint32_t replica_id_cnt = 1;

static replicaset_mapping *
mapping_find(struct tt_uuid *replica_uuid)
{
	struct replicaset_mapping mapping;
	mapping.replica_uuid = *replica_uuid;
	return replicaset_cache_search(&replicas_cache, &mapping);
}

void
print_tree()
{
	struct replicaset_mapping *item = replicaset_cache_first(&replicas_cache);
	struct replicaset_mapping *next;
	int i = 0;
	printf("tree\n");
	for (; item != NULL; item = next) {
		printf("number= %i, %s\n", i++,
		       tt_uuid_str(&item->replica_uuid));
		next = replicaset_cache_next(&replicas_cache, item);
	}
}

static replicaset_mapping *
mapping_new(struct tt_uuid *replica_uuid)
{
	/* TODO: allocate in in mempool.*/
	struct replicaset_mapping *map = (struct replicaset_mapping *)
		calloc(1, sizeof(*map));
	if (map == NULL)
		tnt_raise(OutOfMemory, sizeof(*map), "malloc",
			  "struct replicaset_mapping");

	map->replica_uuid = *replica_uuid;
	map->global_id = replica_id_cnt++;
	replicaset_cache_insert(&replicas_cache, map);
	return map;
}

static inline void
applier_set_state(struct applier *applier, enum applier_state state)
{
	applier->state = state;
	say_debug("=> %s", applier_state_strs[state] +
		  strlen("APPLIER_"));
	trigger_run_xc(&applier->on_state, applier);
}

/**
 * Write a nice error message to log file on SocketError or ClientError
 * in applier_f().
 */
static inline void
applier_log_error(struct applier *applier, struct error *e)
{
	uint32_t errcode = box_error_code(e);
	if (applier->last_logged_errcode == errcode)
		return;
	switch (applier->state) {
	case APPLIER_CONNECT:
		say_info("can't connect to master");
		break;
	case APPLIER_CONNECTED:
	case APPLIER_READY:
		say_info("can't join/subscribe");
		break;
	case APPLIER_AUTH:
		say_info("failed to authenticate");
		break;
	case APPLIER_SYNC:
	case APPLIER_FOLLOW:
	case APPLIER_INITIAL_JOIN:
	case APPLIER_FINAL_JOIN:
		say_info("can't read row");
		break;
	default:
		break;
	}
	error_log(e);
	if (type_cast(SocketError, e) || type_cast(SystemError, e))
		say_info("will retry every %.2lf second",
			 replication_reconnect_timeout());
	applier->last_logged_errcode = errcode;
}

/*
 * Fiber function to write vclock to replication master.
 * To track connection status, replica answers master
 * with encoded vclock. In addition to DML requests,
 * master also sends heartbeat messages every
 * replication_timeout seconds (introduced in 1.7.7).
 * On such requests replica also responds with vclock.
 */
static int
applier_writer_f(va_list ap)
{
	struct applier *applier = va_arg(ap, struct applier *);
	struct ev_io io;
	coio_create(&io, applier->io.fd);

	while (!fiber_is_cancelled()) {
		/*
		 * Tarantool >= 1.7.7 sends periodic heartbeat
		 * messages so we don't need to send ACKs every
		 * replication_timeout seconds any more.
		 */
		if (applier->version_id >= version_id(1, 7, 7))
			fiber_cond_wait_timeout(&applier->writer_cond,
						TIMEOUT_INFINITY);
		else
			fiber_cond_wait_timeout(&applier->writer_cond,
						replication_timeout);
		/* Send ACKs only when in FOLLOW mode ,*/
		if (applier->state != APPLIER_SYNC &&
		    applier->state != APPLIER_FOLLOW)
			continue;
		try {
			struct xrow_header xrow;
			xrow_encode_vclock(&xrow, &replicaset.vclock);
			coio_write_xrow(&io, &xrow);
		} catch (SocketError *e) {
			/*
			 * There is no point trying to send ACKs if
			 * the master closed its end - we would only
			 * spam the log - so exit immediately.
			 */
			if (e->get_errno() == EPIPE)
				break;
			/*
			 * Do not exit, if there is a network error,
			 * the reader fiber will reconnect for us
			 * and signal our cond afterwards.
			 */
			e->log();
		} catch (Exception *e) {
			/*
			 * Out of memory encoding the message, ignore
			 * and try again after an interval.
			 */
			e->log();
		}
		fiber_gc();
	}
	return 0;
}

/**
 * Connect to a remote host and authenticate the client.
 */
void
applier_connect(struct applier *applier)
{
	struct ev_io *coio = &applier->io;
	struct ibuf *ibuf = &applier->ibuf;
	if (coio->fd >= 0)
		return;
	char greetingbuf[IPROTO_GREETING_SIZE];
	struct xrow_header row;

	struct uri *uri = &applier->uri;
	/*
	 * coio_connect() stores resolved address to \a &applier->addr
	 * on success. &applier->addr_len is a value-result argument which
	 * must be initialized to the size of associated buffer (addrstorage)
	 * before calling coio_connect(). Since coio_connect() performs
	 * DNS resolution under the hood it is theoretically possible that
	 * applier->addr_len will be different even for same uri.
	 */
	applier->addr_len = sizeof(applier->addrstorage);
	applier_set_state(applier, APPLIER_CONNECT);
	coio_connect(coio, uri, &applier->addr, &applier->addr_len);
	assert(coio->fd >= 0);
	coio_readn(coio, greetingbuf, IPROTO_GREETING_SIZE);
	applier->last_row_time = ev_monotonic_now(loop());

	/* Decode instance version and name from greeting */
	struct greeting greeting;
	if (greeting_decode(greetingbuf, &greeting) != 0)
		tnt_raise(LoggedError, ER_PROTOCOL, "Invalid greeting");

	if (strcmp(greeting.protocol, "Binary") != 0) {
		tnt_raise(LoggedError, ER_PROTOCOL,
			  "Unsupported protocol for replication");
	}

	if (applier->version_id != greeting.version_id) {
		say_info("remote master is %u.%u.%u at %s\r\n",
			 version_id_major(greeting.version_id),
			 version_id_minor(greeting.version_id),
			 version_id_patch(greeting.version_id),
			 sio_strfaddr(&applier->addr, applier->addr_len));
	}

	/* Save the remote instance version and UUID on connect. */
	applier->uuid = greeting.uuid;
	applier->version_id = greeting.version_id;

	/* Don't display previous error messages in box.info.replication */
	diag_clear(&fiber()->diag);

	/*
	 * Tarantool >= 1.7.7: send an IPROTO_REQUEST_VOTE message
	 * to fetch the master's vclock before proceeding to "join".
	 * It will be used for leader election on bootstrap.
	 */
	if (applier->version_id >= version_id(1, 7, 7)) {
		xrow_encode_request_vote(&row);
		coio_write_xrow(coio, &row);
		coio_read_xrow(coio, ibuf, &row);
		if (row.type != IPROTO_OK)
			xrow_decode_error_xc(&row);
		vclock_create(&applier->vclock);
		xrow_decode_request_vote_xc(&row, &applier->vclock,
					    &applier->remote_is_ro);
	}

	applier_set_state(applier, APPLIER_CONNECTED);

	/* Detect connection to itself */
	if (tt_uuid_is_equal(&applier->uuid, &INSTANCE_UUID))
		tnt_raise(ClientError, ER_CONNECTION_TO_SELF);

	/* Perform authentication if user provided at least login */
	if (!uri->login)
		goto done;

	/* Authenticate */
	applier_set_state(applier, APPLIER_AUTH);
	xrow_encode_auth_xc(&row, greeting.salt, greeting.salt_len, uri->login,
			    uri->login_len, uri->password, uri->password_len);
	coio_write_xrow(coio, &row);
	coio_read_xrow(coio, ibuf, &row);
	applier->last_row_time = ev_monotonic_now(loop());
	if (row.type != IPROTO_OK)
		xrow_decode_error_xc(&row); /* auth failed */

	/* auth succeeded */
	say_info("authenticated");
done:
	applier_set_state(applier, APPLIER_READY);
}

/**
 * Execute and process JOIN request (bootstrap the instance).
 */
static void
applier_join(struct applier *applier)
{
	/* Send JOIN request */
	struct ev_io *coio = &applier->io;
	struct ibuf *ibuf = &applier->ibuf;
	struct xrow_header row;
	xrow_encode_join_xc(&row, &INSTANCE_UUID);
	coio_write_xrow(coio, &row);

	/**
	 * Tarantool < 1.7.0: if JOIN is successful, there is no "OK"
	 * response, but a stream of rows from checkpoint.
	 */
	if (applier->version_id >= version_id(1, 7, 0)) {
		/* Decode JOIN response */
		coio_read_xrow(coio, ibuf, &row);
		if (iproto_type_is_error(row.type)) {
			xrow_decode_error_xc(&row); /* re-throw error */
		} else if (row.type != IPROTO_OK) {
			tnt_raise(ClientError, ER_UNKNOWN_REQUEST_TYPE,
				  (uint32_t) row.type);
		}
		/*
		 * Start vclock. The vclock of the checkpoint
		 * the master is sending to the replica.
		 * Used to initialize the replica's initial
		 * vclock in bootstrap_from_master()
		 */
		xrow_decode_vclock_xc(&row, &replicaset.vclock);
	}

	applier_set_state(applier, APPLIER_INITIAL_JOIN);

	/*
	 * Receive initial data.
	 */
	assert(applier->join_stream != NULL);
	while (true) {
		coio_read_xrow(coio, ibuf, &row);
		applier->last_row_time = ev_monotonic_now(loop());
		if (iproto_type_is_dml(row.type)) {
			xstream_write_xc(applier->join_stream, &row);
		} else if (row.type == IPROTO_OK) {
			if (applier->version_id < version_id(1, 7, 0)) {
				/*
				 * This is the start vclock if the
				 * server is 1.6. Since we have
				 * not initialized replication
				 * vclock yet, do it now. In 1.7+
				 * this vclock is not used.
				 */
				xrow_decode_vclock_xc(&row, &replicaset.vclock);
			}
			break; /* end of stream */
		} else if (iproto_type_is_error(row.type)) {
			xrow_decode_error_xc(&row);  /* rethrow error */
		} else {
			tnt_raise(ClientError, ER_UNKNOWN_REQUEST_TYPE,
				  (uint32_t) row.type);
		}
	}
	say_info("initial data received");

	applier_set_state(applier, APPLIER_FINAL_JOIN);

	/*
	 * Tarantool < 1.7.0: there is no "final join" stage.
	 * Proceed to "subscribe" and do not finish bootstrap
	 * until replica id is received.
	 */
	if (applier->version_id < version_id(1, 7, 0))
		return;

	/*
	 * Receive final data.
	 */
	while (true) {
		coio_read_xrow(coio, ibuf, &row);
		applier->last_row_time = ev_monotonic_now(loop());
		if (iproto_type_is_dml(row.type)) {
			vclock_follow(&replicaset.vclock, row.replica_id,
				      row.lsn);
			xstream_write_xc(applier->subscribe_stream, &row);
		} else if (row.type == IPROTO_OK) {
			/*
			 * Current vclock. This is not used now,
			 * ignore.
			 */
			break; /* end of stream */
		} else if (iproto_type_is_error(row.type)) {
			xrow_decode_error_xc(&row);  /* rethrow error */
		} else {
			tnt_raise(ClientError, ER_UNKNOWN_REQUEST_TYPE,
				  (uint32_t) row.type);
		}
	}
	say_info("final data received");

	applier_set_state(applier, APPLIER_JOINED);
	applier_set_state(applier, APPLIER_READY);
}
const int MAX_REPLICASET_NUMBER = 16;
static struct replicaset_mapping *replicaset_repres_recovery[MAX_REPLICASET_NUMBER] = {};

/**
 * Recovering from _cluster space.
 * This method is called from on_replace trigger.
 * @param global_id
 * @param uuid
 * @param local_id
 * @param replicaset_id
 */
void
deserialize_cluster(uint32_t global_id, struct tt_uuid *uuid, uint32_t local_id,
		    uint32_t replicaset_id)
{
	struct replicaset_mapping *repres_mapping =
		replicaset_repres_recovery[replicaset_id];
	if (repres_mapping == NULL) {
		repres_mapping = mapping_new(uuid);
		repres_mapping->nodes = (struct replicaset_mapping **)
			calloc(1, sizeof(*repres_mapping->nodes));
		repres_mapping->global_id = global_id;
		replicaset_repres_recovery[replicaset_id] = repres_mapping;
		return;
	}
	if (repres_mapping->nodes[local_id] == NULL) {
		/* Already recovered.*/
		return;
	}
	struct replicaset_mapping *new_map = mapping_new(uuid);
	repres_mapping->nodes[local_id] = new_map;
	new_map->nodes = repres_mapping->nodes;
	new_map->global_id = global_id;
}

//static void
//serialize_cluster()
//{
//	struct replicaset_mapping *item, *next, *cur;
//	bool serialized[VCLOCK_MAX] = {};
//	int replica_id = 0;
//	struct space *space = space_cache_find_xc(BOX_CLUSTER_ID);
//	char *data = (char *) malloc(mp_sizeof_array(1)
//				     + mp_sizeof_str(UUID_STR_LEN));
//	if (data == NULL)
//		tnt_raise(OutOfMemory, sizeof(char), "malloc",
//			  "char");
//	char *end;
//
//	space_run_triggers(space, false);
//	for (item = replicaset_cache_first(&replicas_cache); item != NULL;
//	     item = next) {
//		next = replicaset_cache_next(&replicas_cache, item);
//		if (serialized[item->global_id])
//			continue;
//		replica_id++;
//		for (int local_id = 0; local_id < VCLOCK_MAX; local_id++) {
//			cur = item->nodes[local_id];
//			if (cur == NULL)
//				continue;
//			char *uuid_str = tt_uuid_str(&cur->replica_uuid);
//			end = data;
//			end = mp_encode_array(end, 1);
//			end = mp_encode_str(end, uuid_str, UUID_STR_LEN);
//			/* Remove matching uuids. */
//			if (box_delete(BOX_CLUSTER_ID, 1, data, end, NULL) < 0)
//				diag_raise();
//
//			if (boxk(IPROTO_REPLACE, BOX_CLUSTER_ID, "[%u%s%u%u]",
//				cur->global_id, uuid_str,
//				local_id, replica_id))
//				diag_raise();
//			serialized[cur->global_id] = true;
//		}
//	}
//	space_run_triggers(space, true);
//}

static void
update_cluster(struct tt_uuid *uuid,
	       replicaset_mapping  **current_mapping, uint32_t local_id)
{
	struct replicaset_mapping *mapping_entry =
		mapping_find(uuid);
	if (mapping_entry == NULL) {
		mapping_entry = mapping_new(uuid);
	}
	current_mapping[local_id] = mapping_entry;
	/* Save pointer to mapping in each node of replicaset. */
	mapping_entry->nodes = current_mapping;
}

void adapt_vclock(struct replicaset_mapping **nodes, struct vclock *vclock)
{
	struct replicaset_mapping *cur;
	for (uint32_t i = 0; i < VCLOCK_MAX; i++) {
		cur = nodes[i];
		if (cur == NULL)
			continue;
		vclock_set(vclock, i, vclock_get(&replicaset.vclock,
						 cur->global_id));
	}
}

/**
 * Execute and process SUBSCRIBE request (follow updates from a master).
 */
static void
applier_subscribe(struct applier *applier)
{
	assert(applier->subscribe_stream != NULL);
	/* Send SUBSCRIBE request */
	struct ev_io *coio = &applier->io;
	struct ibuf *ibuf = &applier->ibuf;
	struct xrow_header row;
	struct vclock remote_vclock_at_subscribe, adapted_vclock;

	struct replicaset_mapping *mapping_entry = mapping_find(&applier->uuid);
	if (mapping_entry == NULL) {
		mapping_entry = mapping_new(&applier->uuid);
		mapping_entry->nodes = (struct replicaset_mapping **)
					calloc(VCLOCK_MAX,
					       sizeof(*mapping_entry->nodes));
		if (mapping_entry->nodes == NULL)
			tnt_raise(OutOfMemory, VCLOCK_MAX, "malloc",
				  "uint32_t");
	}
	uint32_t crc32c = 0;
	replicaset_mapping **nodes = mapping_entry->nodes;
	/* Calculate checksum of cluster info */
	for (uint32_t i = 0; i < VCLOCK_MAX; i++) {
		if (nodes[i] != NULL) {
			uint32_t local_id = htonl(i);
			crc32c = crc32_calc(crc32c, (char *)&local_id,
					    sizeof(local_id)/ sizeof(char));
			crc32c = crc32_calc(crc32c,
					    tt_uuid_str(&nodes[i]->replica_uuid),
					    UUID_STR_LEN);
		}
	}
	vclock_create(&adapted_vclock);
	adapt_vclock(nodes, &adapted_vclock);

	xrow_encode_subscribe_xc(&row, &REPLICASET_UUID, &INSTANCE_UUID,
				 &adapted_vclock, crc32c);
	coio_write_xrow(coio, &row);

	if (applier->version_id >= version_id(1, 10, 0)) {
		coio_read_xrow(coio, ibuf, &row);
		if (iproto_type_is_error(row.type)) {
			xrow_decode_error_xc(&row);
		} else if (row.type != IPROTO_OK) {
			tnt_raise(ClientError, ER_PROTOCOL,
				  "Invalid cluster info response");
		}
		uint32_t cluster_len = 0;
		tt_uuid uuid;
		xrow_decode_replica(&row, &uuid, &cluster_len);
		if (cluster_len > 0) {
			/* Update first entry */
			update_cluster(&uuid, nodes, row.replica_id);
			for (uint32_t i = 1; i < cluster_len; i++) {
				coio_read_xrow(coio, ibuf, &row);
				xrow_decode_replica(&row, &uuid, NULL);
				update_cluster(&uuid, nodes, row.replica_id);
			}
//			serialize_cluster();
		}
	}

	if (applier->state == APPLIER_READY) {
		/*
		 * Tarantool < 1.7.7 does not send periodic heartbeat
		 * messages so we cannot enable applier synchronization
		 * for it without risking getting stuck in the 'orphan'
		 * mode until a DML operation happens on the master.
		 */
		if (applier->version_id >= version_id(1, 7, 7))
			applier_set_state(applier, APPLIER_SYNC);
		else
			applier_set_state(applier, APPLIER_FOLLOW);
	} else {
		/*
		 * Tarantool < 1.7.0 sends replica id during
		 * "subscribe" stage. We can't finish bootstrap
		 * until it is received.
		 */
		assert(applier->state == APPLIER_FINAL_JOIN);
		assert(applier->version_id < version_id(1, 7, 0));
	}

	/*
	 * Read SUBSCRIBE response
	 */
	if (applier->version_id >= version_id(1, 6, 7)) {
		coio_read_xrow(coio, ibuf, &row);
		if (iproto_type_is_error(row.type)) {
			xrow_decode_error_xc(&row);  /* error */
		} else if (row.type != IPROTO_OK) {
			tnt_raise(ClientError, ER_PROTOCOL,
				  "Invalid response to SUBSCRIBE");
		}
		/*
		 * In case of successful subscribe, the server
		 * responds with its current vclock.
		 */
		vclock_create(&remote_vclock_at_subscribe);
		xrow_decode_vclock_xc(&row, &remote_vclock_at_subscribe);
		adapt_vclock(nodes, &remote_vclock_at_subscribe);
		bool is_empty = true;
		for (uint32_t i = 1; i < VCLOCK_MAX; i++) {
			replicaset_mapping *map = nodes[i];
			if (map == NULL)
				continue;
			if (vclock_get(&replicaset.vclock, map->global_id) > 0) {
				is_empty = false;
				break;
			}
		}
		if (is_empty) {
			for (uint32_t i = 1; i < VCLOCK_MAX; i++) {
				replicaset_mapping *map = nodes[i];
				if (map == NULL)
					continue;
				vclock_set(&replicaset.vclock, map->global_id,
					 vclock_get(&remote_vclock_at_subscribe,
						    i));
			}
		}
	}
	/**
	 * Tarantool < 1.6.7:
	 * If there is an error in subscribe, it's sent directly
	 * in response to subscribe.  If subscribe is successful,
	 * there is no "OK" response, but a stream of rows from
	 * the binary log.
	 */

	/* Re-enable warnings after successful execution of SUBSCRIBE */
	applier->last_logged_errcode = 0;
	if (applier->version_id >= version_id(1, 7, 4)) {
		/* Enable replication ACKs for newer servers */
		assert(applier->writer == NULL);

		char name[FIBER_NAME_MAX];
		int pos = snprintf(name, sizeof(name), "applierw/");
		uri_format(name + pos, sizeof(name) - pos, &applier->uri, false);

		applier->writer = fiber_new_xc(name, applier_writer_f);
		fiber_set_joinable(applier->writer, true);
		fiber_start(applier->writer, applier);
	}

	applier->lag = TIMEOUT_INFINITY;

	/*
	 * Process a stream of rows from the binary log.
	 */
	while (true) {
		if (applier->state == APPLIER_FINAL_JOIN &&
		    instance_id != REPLICA_ID_NIL) {
			say_info("final data received");
			applier_set_state(applier, APPLIER_JOINED);
			applier_set_state(applier, APPLIER_READY);
			applier_set_state(applier, APPLIER_FOLLOW);
		}

		/*
		 * Stay 'orphan' until appliers catch up with
		 * the remote vclock at the time of SUBSCRIBE
		 * and the lag is less than configured.
		 */
		if (applier->state == APPLIER_SYNC &&
		    applier->lag <= replication_sync_lag &&
		    vclock_compare(&remote_vclock_at_subscribe,
				   &replicaset.vclock) <= 0) {
			/* Applier is synced, switch to "follow". */
			applier_set_state(applier, APPLIER_FOLLOW);
		}

		/*
		 * Tarantool < 1.7.7 does not send periodic heartbeat
		 * messages so we can't assume that if we haven't heard
		 * from the master for quite a while the connection is
		 * broken - the master might just be idle.
		 */
		if (applier->version_id < version_id(1, 7, 7)) {
			coio_read_xrow(coio, ibuf, &row);
		} else {
			double timeout = replication_disconnect_timeout();
			coio_read_xrow_timeout_xc(coio, ibuf, &row, timeout);
		}

		if (iproto_type_is_error(row.type))
			xrow_decode_error_xc(&row);  /* error */
		/* Replication request. */
		if (row.replica_id == REPLICA_ID_NIL ||
		    row.replica_id >= VCLOCK_MAX) {
			/*
			 * A safety net, this can only occur
			 * if we're fed a strangely broken xlog.
			 */
			tnt_raise(ClientError, ER_UNKNOWN_REPLICA,
				  int2str(row.replica_id),
				  tt_uuid_str(&REPLICASET_UUID));
		}
		row.replica_id = nodes[row.replica_id]->global_id;
		applier->lag = ev_now(loop()) - row.tm;
		applier->last_row_time = ev_monotonic_now(loop());

		if (vclock_get(&replicaset.vclock, row.replica_id) < row.lsn) {
			/**
			 * Promote the replica set vclock before
			 * applying the row. If there is an
			 * exception (conflict) applying the row,
			 * the row is skipped when the replication
			 * is resumed.
			 */
			vclock_follow(&replicaset.vclock, row.replica_id,
				      row.lsn);
			if (xstream_write(applier->subscribe_stream, &row) != 0) {
				struct error *e = diag_last_error(diag_get());
				/**
				 * Silently skip ER_TUPLE_FOUND error if such
				 * option is set in config.
				 */
				if (e->type == &type_ClientError &&
				    box_error_code(e) == ER_TUPLE_FOUND &&
				    cfg_geti("replication_skip_conflict"))
					diag_clear(diag_get());
				else
					diag_raise();
			}
		}
		if (applier->state == APPLIER_SYNC ||
		    applier->state == APPLIER_FOLLOW)
			fiber_cond_signal(&applier->writer_cond);
		if (ibuf_used(ibuf) == 0)
			ibuf_reset(ibuf);
		fiber_gc();
	}
}

static inline void
applier_disconnect(struct applier *applier, enum applier_state state)
{
	applier_set_state(applier, state);
	if (applier->writer != NULL) {
		fiber_cancel(applier->writer);
		fiber_join(applier->writer);
		applier->writer = NULL;
	}

	coio_close(loop(), &applier->io);
	/* Clear all unparsed input. */
	ibuf_reinit(&applier->ibuf);
	fiber_gc();
}

static int
applier_f(va_list ap)
{
	struct applier *applier = va_arg(ap, struct applier *);
	/*
	 * Set correct session type for use in on_replace()
	 * triggers.
	 */
	current_session()->type = SESSION_TYPE_APPLIER;

	/* Re-connect loop */
	while (!fiber_is_cancelled()) {
		try {
			applier_connect(applier);
			if (tt_uuid_is_nil(&REPLICASET_UUID)) {
				/*
				 * Execute JOIN if this is a bootstrap.
				 * The join will pause the applier
				 * until WAL is created.
				 */
				applier_join(applier);
			}
			applier_subscribe(applier);
			/*
			 * subscribe() has an infinite loop which
			 * is stoppable only with fiber_cancel().
			 */
			unreachable();
			return 0;
		} catch (ClientError *e) {
			if (e->errcode() == ER_CONNECTION_TO_SELF &&
			    tt_uuid_is_equal(&applier->uuid, &INSTANCE_UUID)) {
				/* Connection to itself, stop applier */
				applier_disconnect(applier, APPLIER_OFF);
				return 0;
			} else if (e->errcode() == ER_LOADING) {
				/* Autobootstrap */
				applier_log_error(applier, e);
				goto reconnect;
			} else if (e->errcode() == ER_ACCESS_DENIED) {
				/* Invalid configuration */
				applier_log_error(applier, e);
				goto reconnect;
			} else if (e->errcode() == ER_SYSTEM) {
				/* System error from master instance. */
				applier_log_error(applier, e);
				goto reconnect;
			} else if (e->errcode() == ER_CFG) {
				/* Invalid configuration */
				applier_log_error(applier, e);
				goto reconnect;
			} else {
				/* Unrecoverable errors */
				applier_log_error(applier, e);
				applier_disconnect(applier, APPLIER_STOPPED);
				return -1;
			}
		} catch (FiberIsCancelled *e) {
			applier_disconnect(applier, APPLIER_OFF);
			break;
		} catch (SocketError *e) {
			applier_log_error(applier, e);
			goto reconnect;
		} catch (SystemError *e) {
			applier_log_error(applier, e);
			goto reconnect;
		} catch (ChannelIsClosed *e) {
			applier_disconnect(applier, APPLIER_OFF);
			break;
		} catch (Exception *e) {
			applier_log_error(applier, e);
			applier_disconnect(applier, APPLIER_STOPPED);
			return -1;
		}
		/* Put fiber_sleep() out of catch block.
		 *
		 * This is done to avoid the case when two or more
		 * fibers yield inside their try/catch blocks and
		 * throw an exception. Seems like the exception unwinder
		 * uses global state inside the catch block.
		 *
		 * This could lead to incorrect exception processing
		 * and crash the program.
		 *
		 * See: https://github.com/tarantool/tarantool/issues/136
		*/
reconnect:
		applier_disconnect(applier, APPLIER_DISCONNECTED);
		fiber_sleep(replication_reconnect_timeout());
	}
	return 0;
}

void
applier_start(struct applier *applier)
{
	char name[FIBER_NAME_MAX];
	assert(applier->reader == NULL);

	int pos = snprintf(name, sizeof(name), "applier/");
	uri_format(name + pos, sizeof(name) - pos, &applier->uri, false);

	struct fiber *f = fiber_new_xc(name, applier_f);
	/**
	 * So that we can safely grab the status of the
	 * fiber any time we want.
	 */
	fiber_set_joinable(f, true);
	applier->reader = f;
	fiber_start(f, applier);
}

void
applier_stop(struct applier *applier)
{
	struct fiber *f = applier->reader;
	if (f == NULL)
		return;
	fiber_cancel(f);
	fiber_join(f);
	applier_set_state(applier, APPLIER_OFF);
	applier->reader = NULL;
}

struct applier *
applier_new(const char *uri, struct xstream *join_stream,
	    struct xstream *subscribe_stream)
{
	struct applier *applier = (struct applier *)
		calloc(1, sizeof(struct applier));
	if (applier == NULL) {
		diag_set(OutOfMemory, sizeof(*applier), "malloc",
			 "struct applier");
		return NULL;
	}
	coio_create(&applier->io, -1);
	ibuf_create(&applier->ibuf, &cord()->slabc, 1024);

	/* uri_parse() sets pointers to applier->source buffer */
	snprintf(applier->source, sizeof(applier->source), "%s", uri);
	int rc = uri_parse(&applier->uri, applier->source);
	/* URI checked by box_check_replication() */
	assert(rc == 0 && applier->uri.service != NULL);
	(void) rc;

	applier->join_stream = join_stream;
	applier->subscribe_stream = subscribe_stream;
	applier->last_row_time = ev_monotonic_now(loop());
	rlist_create(&applier->on_state);
	fiber_cond_create(&applier->resume_cond);
	fiber_cond_create(&applier->writer_cond);

	return applier;
}

void
applier_delete(struct applier *applier)
{
	assert(applier->reader == NULL && applier->writer == NULL);
	ibuf_destroy(&applier->ibuf);
	assert(applier->io.fd == -1);
	trigger_destroy(&applier->on_state);
	fiber_cond_destroy(&applier->resume_cond);
	fiber_cond_destroy(&applier->writer_cond);
	free(applier);
}

void
applier_resume(struct applier *applier)
{
	assert(!fiber_is_dead(applier->reader));
	applier->is_paused = false;
	fiber_cond_signal(&applier->resume_cond);
}

void
applier_pause(struct applier *applier)
{
	/* Sleep until applier_resume() wake us up */
	assert(fiber() == applier->reader);
	assert(!applier->is_paused);
	applier->is_paused = true;
	while (applier->is_paused && !fiber_is_cancelled())
		fiber_cond_wait(&applier->resume_cond);
}

struct applier_on_state {
	struct trigger base;
	struct applier *applier;
	enum applier_state desired_state;
	struct fiber_cond wakeup;
};

static void
applier_on_state_f(struct trigger *trigger, void *event)
{
	(void) event;
	struct applier_on_state *on_state =
		container_of(trigger, struct applier_on_state, base);

	struct applier *applier = on_state->applier;

	if (applier->state != APPLIER_OFF &&
	    applier->state != APPLIER_STOPPED &&
	    applier->state != on_state->desired_state)
		return;

	/* Wake up waiter */
	fiber_cond_signal(&on_state->wakeup);

	applier_pause(applier);
}

static inline void
applier_add_on_state(struct applier *applier,
		     struct applier_on_state *trigger,
		     enum applier_state desired_state)
{
	trigger_create(&trigger->base, applier_on_state_f, NULL, NULL);
	trigger->applier = applier;
	fiber_cond_create(&trigger->wakeup);
	trigger->desired_state = desired_state;
	trigger_add(&applier->on_state, &trigger->base);
}

static inline void
applier_clear_on_state(struct applier_on_state *trigger)
{
	fiber_cond_destroy(&trigger->wakeup);
	trigger_clear(&trigger->base);
}

static inline int
applier_wait_for_state(struct applier_on_state *trigger, double timeout)
{
	struct applier *applier = trigger->applier;
	double deadline = ev_monotonic_now(loop()) + timeout;
	while (applier->state != APPLIER_OFF &&
	       applier->state != APPLIER_STOPPED &&
	       applier->state != trigger->desired_state) {
		if (fiber_cond_wait_deadline(&trigger->wakeup, deadline) != 0)
			return -1; /* ER_TIMEOUT */
	}
	if (applier->state != trigger->desired_state) {
		assert(applier->state == APPLIER_OFF ||
		       applier->state == APPLIER_STOPPED);
		/* Re-throw the original error */
		assert(!diag_is_empty(&applier->reader->diag));
		diag_move(&applier->reader->diag, &fiber()->diag);
		return -1;
	}
	return 0;
}

void
applier_resume_to_state(struct applier *applier, enum applier_state state,
			double timeout)
{
	struct applier_on_state trigger;
	applier_add_on_state(applier, &trigger, state);
	applier_resume(applier);
	int rc = applier_wait_for_state(&trigger, timeout);
	applier_clear_on_state(&trigger);
	if (rc != 0)
		diag_raise();
	assert(applier->state == state);
}
