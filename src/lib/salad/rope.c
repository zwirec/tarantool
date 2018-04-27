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
/*
 * Copyright (c) 1993-1994 by Xerox Corporation.  All rights reserved.
 *
 * THIS MATERIAL IS PROVIDED AS IS, WITH ABSOLUTELY NO WARRANTY EXPRESSED
 * OR IMPLIED.  ANY USE IS AT YOUR OWN RISK.
 *
 * Permission is hereby granted to use or copy this program
 * for any purpose,  provided the above notices are retained on all copies.
 * Permission to modify the code and to distribute modified code is granted,
 * provided the above notices are retained, and a notice that the code was
 * modified is included with the above copyright notice.
 *
 * Author: Hans-J. Boehm (boehm@parc.xerox.com)
 */

#define ROPE_SRC
#include "rope.h"

static inline void
rope_relink(struct rope_node *node)
{
	node->tree_size = (rope_node_size(node->link[0]) +
			   rope_node_size(node->link[1]) +
			   node->leaf_size);
	node->height = MAX(rope_node_height(node->link[0]),
			   rope_node_height(node->link[1])) + 1;
}

static inline struct rope_node *
avl_rotate_single(struct rope_node *parent, int direction)
{
	struct rope_node *save = parent->link[!direction];

	parent->link[!direction] = save->link[direction];
	save->link[direction] = parent;

	/* First relink the parent, since it's now a child. */
	rope_relink(parent);
	rope_relink(save);

	return save;
}

static inline struct rope_node *
avl_rotate_double(struct rope_node *parent, int direction)
{
	parent->link[!direction] =
		avl_rotate_single(parent->link[!direction], !direction);
	return avl_rotate_single(parent, direction);
}

void
avl_rebalance_after_insert(struct rope_node ***path, struct rope_node ***p_end,
			   int insert_height)
{
	while (p_end > path) {
		struct rope_node *left = **p_end--;
		struct rope_node *parent = **p_end;
		/*
		 * To use the same rotation functions, set mirror
		 * to 1 if left is right and right is left.
		 */
		int mirror = left != parent->link[0];
		struct rope_node *right = parent->link[!mirror];

		int left_height = rope_node_height(left);
		int right_height = rope_node_height(right);
		parent->height = MAX(left_height, right_height) + 1;
		/*
		 * Rotations flattened the tree, so there is no
		 * further changes in height up the insertion
		 * path.
		 */
		if (left_height == right_height)
			break;
		/*
		 * We've been adding a new child (children) to the
		 * 'left' subtree, so it couldn't get shorter.
		 * The old difference between subtrees was in the
		 * range -1..1. So the new difference can only be
		 * in the range -1..1 + height(new_node).
		 */
		if (left_height - right_height >= 2) {
			struct rope_node *l_left = left->link[mirror];
			struct rope_node *l_right = left->link[!mirror];
			int l_left_height = rope_node_height(l_left);
			int l_right_height = rope_node_height(l_right);
			/*
			 * Rotate in the direction, opposite to
			 * the skew. E.g. if we have two left-left
			 * nodes hanging off the tree, rotate the
			 * parent clockwise. If we have a left
			 * node with a right child, rotate the
			 * child counterclockwise, and then the whole
			 * thing clockwise.
			 */
			if (l_left_height >= l_right_height)
				**p_end = avl_rotate_single(parent,
							    !mirror);
			else
				**p_end = avl_rotate_double(parent,
							    !mirror);
			/*
			 * If we inserted only one node, no more
			 * than 1 rotation is required (see
			 * D. Knuth, Introduction to Algorithms,
			 * vol. 3.). For 2 nodes, its max
			 * 2 rotations.
			 */
			if (l_left_height != l_right_height &&
			    --insert_height == 0)
				break;
		}
	}
}

void
avl_rebalance_after_delete(struct rope_node ***path, struct rope_node ***p_end)
{
	while (p_end > path) {
		struct rope_node *left = **p_end--;
		struct rope_node *parent = **p_end;

		int mirror = left != parent->link[0];

		struct rope_node *right = parent->link[!mirror];

		int left_height = rope_node_height(left);
		int right_height = rope_node_height(right);

		parent->height = MAX(left_height, right_height) + 1;
		/*
		 * Right was taller, and we deleted from the left.
		 * We can break the loop since there can be no
		 * changes in height up in the route.
		 */
		if (left_height - right_height == -1)
			break;

		if (left_height - right_height <= -2) {
			struct rope_node *r_left = right->link[mirror];
			struct rope_node *r_right = right->link[!mirror];
			int r_left_height = rope_node_height(r_left);
			int r_right_height = rope_node_height(r_right);

			if (r_left_height <= r_right_height)
				**p_end = avl_rotate_single(parent, mirror);
			else
				**p_end = avl_rotate_double(parent, mirror);
		}
	}
}

struct rope_node ***
avl_route_to_offset(struct rope_node ***path, rope_size_t *p_offset,
		    ssize_t adjust_size)
{
	rope_size_t offset = *p_offset;
	while (**path) {
		struct rope_node *node = **path;

		node->tree_size += adjust_size;

		rope_size_t left_size = rope_node_size(node->link[0]);

		if (offset < left_size) {
			/* The offset lays in  the left subtree. */
			*++path = &node->link[0];
		} else {
			/* Make the new offset relative to the parent. */
			offset -= left_size;

			if (offset < node->leaf_size) {
				/* Found. */
				break;
			} else {
				/*
				 * Make the offset relative to the
				 * leftmost node in the right subtree.
				 */
				offset -= node->leaf_size;
			}
			*++path = &node->link[1];
		}
	}
	*p_offset = offset;
	return path;
}

struct rope_node ***
avl_route_to_next(struct rope_node ***path, int dir, rope_ssize_t adjust_size)
{
	struct rope_node *node = **path;
	*++path = &node->link[dir];
	while (**path) {
		node = **path;
		node->tree_size += adjust_size;
		*++path = &node->link[!dir];
	}
	return path;
}
