#ifndef INCLUDES_TARANTOOL_BOX_PROMOTE_H
#define INCLUDES_TARANTOOL_BOX_PROMOTE_H
/*
 * Copyright 2010-2018, Tarantool AUTHORS, please see AUTHORS file.
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
#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

struct info_handler;
struct promote_msg;

/**
 * Decode the MessagePack encoded promotion message.
 * @param data MessagePack data to decode. Tuple from _promotion.
 *
 * @retval NULL Error during decoding.
 * @retval not NULL Decoded message.
 */
struct promote_msg *
promote_msg_new_from_tuple(const char *data);

/** Schedule the promotion message processing. */
void
promote_msg_push(struct promote_msg *msg);

/** Free the promotion message memory. */
void
promote_msg_delete(struct promote_msg *msg);

/**
 * Promote the current instance to be a master in the fullmesh
 * master-master cluster. The old master, if exists, is demoted.
 * Once a promotion attempt is done anywhere, change of
 * box.cfg.read_only flag is disabled.
 * @param timeout Timeout during which the promotion should be
 *        finished.
 * @param quorum The promotion quorum of instances who should
 *        approve the promotion and sync with the old master
 *        before demotion. The quorum should be at least half of
 *        the cluster size + 1 and include the old master. If an
 *        old master does not exist, then the quorum is ignored
 *        and the promotion waits for 100% of the cluster
 *        members.
 *
 * @retval -1 Error.
 * @retval 0 Success.
 */
int
box_ctl_promote(double timeout, int quorum);

/**
 * Demote the current instance. Works the same as
 * box.ctl.promote() but the round is stopped after demotion.
 */
int
box_ctl_demote(double timeout, int quorum);

/**
 * Perform an initial round of promotion on bootstrap when no
 * masters in the cluster.
 */
int
box_ctl_initial_promote(void);

/**
 * Show status of the currently active promotion round or the last
 * finished one.
 * @param info Info handler to collect the info into.
 */
void
box_ctl_promote_info(struct info_handler *info);

/**
 * Remove all the promotion rounds from the history. That allows
 * to change read_only manually again.
 */
int
box_ctl_promote_reset(void);

/** Check if a promotion history is empty. */
bool
promote_is_empty(void);

/** Initialize the promotion subsystem. */
int
box_ctl_promote_init(void);

/** Free promote resources. */
void
box_ctl_promote_free(void);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* INCLUDES_TARANTOOL_BOX_PROMOTE_H */
