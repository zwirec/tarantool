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
#include "memtx_tree.h"
#include "memtx_engine.h"
#include "tuple_compare.h"

/**
 * Set key and part count and recalculate internal
 * depending fields.
 */
static inline void
memtx_tree_key_data_set(struct memtx_tree_key_data *key_data,
			const char *key, uint32_t part_count,
			const struct key_def *key_def)
{
	(void)key_def;
	key_data->key = key;
	key_data->part_count = part_count;
}

/** Set tuple and recalculate internal depending fields */
static inline void
memtx_tree_data_set(struct memtx_tree_data *data, struct tuple *tuple,
		    const struct key_def *key_def)
{
	(void)key_def;
	data->tuple = tuple;
}

static inline bool
memtx_tree_data_equal(const struct memtx_tree_data *a,
		      const struct memtx_tree_data *b)
{
	return a->tuple == b->tuple;
}

static inline int
memtx_tree_data_compare(const struct memtx_tree_data *a,
			const struct memtx_tree_data *b,
			struct key_def *key_def)
{
	return tuple_compare(a->tuple, b->tuple, key_def);
}

static inline int
memtx_tree_data_compare_with_key(const struct memtx_tree_data *a,
				 struct memtx_tree_key_data *key,
				 struct key_def *key_def)
{
	assert(key->part_count != 0);
	return tuple_compare_with_key(a->tuple, key->key, key->part_count,
				      key_def);
}

#define MEMTX_TREE_NAME metmx_normal_tree
#define MEMTX_TREE_ELEM_SET(elem, tuple, key_def) memtx_tree_data_set(elem, tuple, key_def)
#define MEMTX_TREE_KEY_SET(elem, key, part_count, key_def) memtx_tree_key_data_set(elem, key, part_count, key_def)
#define MEMTX_TREE_EQUAL(a, b) memtx_tree_data_equal(&a, &b)
#define MEMTX_TREE_COMPARE(a, b, arg) memtx_tree_data_compare(&a, &b, arg)
#define MEMTX_TREE_COMPARE_KEY(a, b, arg) memtx_tree_data_compare_with_key(&a, b, arg)
#define memtx_tree_elem struct memtx_tree_data
#define memtx_tree_key struct memtx_tree_key_data

#include "memtx_tree_impl.h"

#undef MEMTX_TREE_NAME
#undef MEMTX_TREE_ELEM_SET
#undef MEMTX_TREE_KEY_SET
#undef MEMTX_TREE_EQUAL
#undef MEMTX_TREE_COMPARE
#undef MEMTX_TREE_COMPARE_KEY
#undef memtx_tree_elem
#undef memtx_tree_key

struct memtx_hinted_tree_data {
	struct memtx_tree_data base;
	/**
	 * Compare hint.
	 * Is calculated automatically in 'set' method.
	 */
	uint64_t hint;
};

struct memtx_hinted_tree_key_data {
	struct memtx_tree_key_data base;
	uint64_t hint;
};

/**
 * Set key and part count and recalculate internal
 * depending fields.
 */
static inline void
memtx_hinted_tree_key_data_set(struct memtx_hinted_tree_key_data *key_data,
			       const char *key, uint32_t part_count,
			       const struct key_def *key_def)
{
	key_data->base.key = key;
	key_data->base.part_count = part_count;
	if (part_count > 0)
		key_data->hint = key_hint(key, key_def);
	else
		key_data->hint = 0;
}

/** Set tuple and recalculate internal depending fields */
static inline void
memtx_hinted_tree_data_set(struct memtx_hinted_tree_data *data,
			   struct tuple *tuple, const struct key_def *key_def)
{
	data->base.tuple = tuple;
	data->hint = tuple_hint(tuple, key_def);
}

static inline bool
memtx_hinted_tree_data_equal(const struct memtx_hinted_tree_data *a,
			     const struct memtx_hinted_tree_data *b)
{
	return a->base.tuple == b->base.tuple;
}

static inline int
memtx_hinted_tree_data_compare(const struct memtx_hinted_tree_data *a,
			       const struct memtx_hinted_tree_data *b,
			       struct key_def *key_def)
{
	if (a->hint != b->hint)
		return a->hint < b->hint ? -1 : 1;
	return tuple_compare(a->base.tuple, b->base.tuple, key_def);
}

static inline int
memtx_hinted_tree_data_compare_with_key(const struct memtx_hinted_tree_data *a,
				        struct memtx_hinted_tree_key_data *key,
				        struct key_def *key_def)
{
	assert(key->base.part_count != 0);
	if (a->hint != key->hint)
		return a->hint < key->hint ? -1 : 1;
	return tuple_compare_with_key(a->base.tuple, key->base.key,
				      key->base.part_count, key_def);
}

#define MEMTX_TREE_NAME metmx_hinted_tree
#define MEMTX_TREE_ELEM_SET(elem, tuple, key_def) memtx_hinted_tree_data_set(elem, tuple, key_def)
#define MEMTX_TREE_KEY_SET(elem, key, part_count, key_def) memtx_hinted_tree_key_data_set(elem, key, part_count, key_def)
#define MEMTX_TREE_EQUAL(a, b) memtx_hinted_tree_data_equal(&a, &b)
#define MEMTX_TREE_COMPARE(a, b, arg) memtx_hinted_tree_data_compare(&a, &b, arg)
#define MEMTX_TREE_COMPARE_KEY(a, b, arg) memtx_hinted_tree_data_compare_with_key(&a, b, arg)
#define memtx_tree_elem struct memtx_hinted_tree_data
#define memtx_tree_key struct memtx_hinted_tree_key_data

#include "memtx_tree_impl.h"

#undef MEMTX_TREE_NAME
#undef MEMTX_TREE_ELEM_SET
#undef MEMTX_TREE_KEY_SET
#undef MEMTX_TREE_EQUAL
#undef MEMTX_TREE_COMPARE
#undef MEMTX_TREE_COMPARE_KEY
#undef memtx_tree_elem
#undef memtx_tree_key

static inline int
memtx_hint_only_tree_data_compare(const struct memtx_hinted_tree_data *a,
				  const struct memtx_hinted_tree_data *b,
				  struct key_def *key_def)
{
	(void)key_def;
	return a->hint < b->hint ? -1 : a->hint > b->hint;
}

static inline int
memtx_hint_only_tree_data_compare_with_key(const struct memtx_hinted_tree_data *a,
					   struct memtx_hinted_tree_key_data *key,
					   struct key_def *key_def)
{
	(void)key_def;
	return a->hint < key->hint ? -1 : a->hint > key->hint;
}


#define MEMTX_TREE_NAME metmx_hint_only_tree
#define MEMTX_TREE_ELEM_SET(elem, tuple, key_def) memtx_hinted_tree_data_set(elem, tuple, key_def)
#define MEMTX_TREE_KEY_SET(elem, key, part_count, key_def) memtx_hinted_tree_key_data_set(elem, key, part_count, key_def)
#define MEMTX_TREE_EQUAL(a, b) memtx_hinted_tree_data_equal(&a, &b)
#define MEMTX_TREE_COMPARE(a, b, arg) memtx_hint_only_tree_data_compare(&a, &b, arg)
#define MEMTX_TREE_COMPARE_KEY(a, b, arg) memtx_hint_only_tree_data_compare_with_key(&a, b, arg)
#define memtx_tree_elem struct memtx_hinted_tree_data
#define memtx_tree_key struct memtx_hinted_tree_key_data

#include "memtx_tree_impl.h"

#undef MEMTX_TREE_NAME
#undef MEMTX_TREE_ELEM_SET
#undef MEMTX_TREE_KEY_SET
#undef MEMTX_TREE_EQUAL
#undef MEMTX_TREE_COMPARE
#undef MEMTX_TREE_COMPARE_KEY
#undef memtx_tree_elem
#undef memtx_tree_key

struct index *
memtx_tree_index_new(struct memtx_engine *memtx, struct index_def *def)
{
	if (!def->opts.hint)
		return metmx_normal_tree_index_new(memtx, def);
	if (def->cmp_def->part_count == 1 &&
	    (def->cmp_def->parts->type == FIELD_TYPE_UNSIGNED ||
	     def->cmp_def->parts->type == FIELD_TYPE_INTEGER))
		return metmx_hint_only_tree_index_new(memtx, def);
	return metmx_hinted_tree_index_new(memtx, def);
}
