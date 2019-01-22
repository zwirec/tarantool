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

struct memtx_multikey_tree_data {
	struct memtx_tree_data base;
	/**
	 * Multikey item index.
	 * Is calculated automatically in 'set' method.
	 */
	uint64_t multikey_idx;
};

/** Set tuple and recalculate internal depending fields */
static inline void
memtx_multikey_tree_data_set(struct memtx_multikey_tree_data *data,
			     struct tuple *tuple, uint32_t multikey_idx)
{
	data->base.tuple = tuple;
	data->multikey_idx = multikey_idx;
}

static inline bool
memtx_multikey_tree_data_equal(const struct memtx_multikey_tree_data *a,
			       const struct memtx_multikey_tree_data *b)
{
	return a->base.tuple == b->base.tuple &&
	       a->multikey_idx == b->multikey_idx;
}

static inline int
memtx_multikey_tree_data_compare(const struct memtx_multikey_tree_data *a,
				 const struct memtx_multikey_tree_data *b,
				 struct key_def *key_def)
{
	int rc = 0;
	const char *a_key =
		tuple_field_by_part_multikey(a->base.tuple, key_def->parts,
					     a->multikey_idx);
	const char *b_key =
		tuple_field_by_part_multikey(b->base.tuple, key_def->parts,
					     b->multikey_idx);
	assert(a_key != NULL && b_key != NULL);
	rc = tuple_compare_field(a_key, b_key, key_def->parts->type,
				 key_def->parts->coll);
	if (rc != 0 || key_def->part_count == 1)
		return rc;
	a_key = tuple_field_by_part(a->base.tuple, key_def->parts + 1);
	b_key = tuple_field_by_part(b->base.tuple, key_def->parts + 1);
	rc = tuple_compare_field(a_key, b_key, key_def->parts[1].type,
				 key_def->parts[1].coll);
	return rc;
}

static inline int
memtx_multikey_tree_data_compare_with_key(const struct memtx_multikey_tree_data *a,
					  struct memtx_tree_key_data *key,
					  struct key_def *key_def)
{
	// printf("memtx_multikey_tree_data_compare_with_key %p %p [key = %s]\n",
	//        a, key, mp_str(key->key));
	int rc = 0;
	const char *a_key =
		tuple_field_by_part_multikey(a->base.tuple, key_def->parts,
					     a->multikey_idx);
	rc = tuple_compare_field(a_key, key->key, key_def->parts->type,
				 key_def->parts->coll);
	return rc;
}

static int
multikey_index_insert_tuple(struct index *base, struct tuple *tuple,
			    uint32_t multikey_idx, struct tuple **replaced);

static void
multikey_index_delete_tuple(struct index *base, struct tuple *tuple,
			    uint32_t multikey_idx);

static int
memtx_multikey_tree_index_replace(struct index *base, struct tuple *old_tuple,
				  struct tuple *new_tuple,
				  enum dup_replace_mode mode,
				  struct tuple **result);

static int
memtx_multikey_tree_index_build_next(struct index *base, struct tuple *tuple);

#define MEMTX_TREE_NAME memtx_multikey_tree
#define MEMTX_TREE_KEY_SET(elem, key, part_count, key_def) memtx_tree_key_data_set(elem, key, part_count, key_def)
#define MEMTX_TREE_EQUAL(a, b) memtx_multikey_tree_data_equal(&a, &b)
#define MEMTX_TREE_COMPARE(a, b, arg) memtx_multikey_tree_data_compare(&a, &b, arg)
#define MEMTX_TREE_COMPARE_KEY(a, b, arg) memtx_multikey_tree_data_compare_with_key(&a, b, arg)
#define memtx_tree_elem struct memtx_multikey_tree_data
#define memtx_tree_key struct memtx_tree_key_data
#define memtx_tree_index_delete_tuple multikey_index_insert_tuple
#define memtx_tree_index_insert_tuple multikey_index_delete_tuple
#define memtx_tree_index_replace memtx_multikey_tree_index_replace
#define memtx_tree_index_build_next memtx_multikey_tree_index_build_next

#include "memtx_tree_impl.h"

static int
multikey_index_insert_tuple(struct index *base, struct tuple *tuple,
			    uint32_t multikey_idx, struct tuple **replaced)
{
	printf("multikey_index_insert_tuple %s [%d]\n", tuple_str(tuple),
		multikey_idx);
	struct memtx_multikey_tree_index *index = (struct memtx_multikey_tree_index *)base;
	memtx_tree_elem data;
	memtx_tree_elem data_replaced;
	memtx_multikey_tree_data_set(&data, tuple, multikey_idx);
	memset(&data_replaced, 0, sizeof(data_replaced));
	int rc = memtx_multikey_tree_tree_insert(&index->tree, data, &data_replaced);
	if (replaced != NULL)
		*replaced = ((struct memtx_tree_data *)&data_replaced)->tuple;
	return rc;
}

static void
multikey_index_delete_tuple(struct index *base, struct tuple *tuple,
			    uint32_t multikey_idx)
{
	printf("multikey_index_delete_tuple %s [%d]\n", tuple_str(tuple),
		multikey_idx);
	struct memtx_multikey_tree_index *index = (struct memtx_multikey_tree_index *)base;
	memtx_tree_elem data;
	memtx_multikey_tree_data_set(&data, tuple, multikey_idx);
	memtx_multikey_tree_tree_delete(&index->tree, data);
}

static int
memtx_multikey_tree_index_replace(struct index *base, struct tuple *old_tuple,
			 	  struct tuple *new_tuple,
				  enum dup_replace_mode mode,
			 	  struct tuple **result)
{
	printf("memtx_multikey_tree_index_replace old = %s new = %s\n",
	       tuple_str(old_tuple), tuple_str(new_tuple));
	struct memtx_multikey_tree_index *index = (struct memtx_multikey_tree_index *)base;
	struct key_def *key_def = index->tree.arg;
	if (new_tuple != NULL) {
		struct tuple *dup_tuple = NULL;
		const char *field = tuple_field_by_part(new_tuple,
							key_def->parts);
		int size = mp_decode_array(&field);

		/* Try to optimistically replace the new_tuple. */
		int ins_rc = 0;
		uint32_t dup_rc = 0;
		int multikey_idx = 0;
		for (multikey_idx = 0; multikey_idx < size; multikey_idx++) {
			ins_rc = multikey_index_insert_tuple(base, new_tuple,
					multikey_idx, &dup_tuple);
			if (ins_rc != 0) {
				--multikey_idx;
				for (; multikey_idx >= 0; --multikey_idx)
					multikey_index_delete_tuple(base, new_tuple,
							    multikey_idx);
				diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
				 	 "memtx_tree_index", "replace");
				return -1;
			}
			dup_rc = replace_check_dup(old_tuple, dup_tuple, mode);
			if (dup_rc != 0) {
				int svp = multikey_idx;
				for (; multikey_idx >= 0; --multikey_idx)
					multikey_index_delete_tuple(base, new_tuple,
							    multikey_idx);
				if (dup_tuple != new_tuple) {
					multikey_index_insert_tuple(base,
						dup_tuple, svp, NULL);
				}
				struct space *sp =
					space_cache_find(base->def->space_id);
				assert(sp != NULL);
				diag_set(ClientError, dup_rc, base->def->name,
					 space_name(sp));
				return -1;
			}
		}
		if (dup_tuple != NULL) {
			*result = dup_tuple;
			return 0;
		}
	}
	if (old_tuple) {
		const char *field = tuple_field_by_part(old_tuple,
							key_def->parts);
		int size = mp_decode_array(&field);
		for (int i = 0; i < size; i++)
			multikey_index_delete_tuple(base, old_tuple, i);
	}
	*result = old_tuple;
	return 0;
}

static int
memtx_multikey_tree_index_build_next(struct index *base, struct tuple *tuple)
{
	struct memtx_multikey_tree_index *index =
		(struct memtx_multikey_tree_index *)base;
	struct key_def *key_def = index->tree.arg;
	const char *field = tuple_field_by_part(tuple, key_def->parts);
	uint32_t size = mp_decode_array(&field);

	if (index->build_array == NULL) {
		index->build_array =
			(memtx_tree_elem *)malloc(MEMTX_EXTENT_SIZE);
		if (index->build_array == NULL) {
			diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
				 "memtx_tree_index", "build_next");
			return -1;
		}
		index->build_array_alloc_size =
			MEMTX_EXTENT_SIZE / (size * sizeof(index->build_array[0]));
	}
	assert(index->build_array_size <= index->build_array_alloc_size);
	if (index->build_array_size == index->build_array_alloc_size) {
		index->build_array_alloc_size = index->build_array_alloc_size +
				MAX(index->build_array_alloc_size / 2, size);
		memtx_tree_elem *tmp = realloc(index->build_array,
				index->build_array_alloc_size * sizeof(*tmp));
		if (tmp == NULL) {
			diag_set(OutOfMemory, index->build_array_alloc_size *
				 sizeof(*tmp), "memtx_tree_index", "build_next");
			return -1;
		}
		index->build_array = tmp;
	}
	for (uint32_t i = 0; i < size; i++) {
		uint32_t idx = index->build_array_size++;
		memtx_multikey_tree_data_set(&index->build_array[idx], tuple, i);
	}
	return 0;
}

#undef MEMTX_TREE_NAME
#undef MEMTX_TREE_ELEM_SET
#undef MEMTX_TREE_EQUAL
#undef MEMTX_TREE_COMPARE
#undef MEMTX_TREE_COMPARE_KEY
#undef memtx_tree_elem
#undef memtx_tree_key

struct index *
memtx_tree_index_new(struct memtx_engine *memtx, struct index_def *def)
{
	if (def->opts.is_multikey)
		return memtx_multikey_tree_index_new(memtx, def);
	if (!def->opts.hint)
		return metmx_normal_tree_index_new(memtx, def);
	if (def->cmp_def->part_count == 1 &&
	    (def->cmp_def->parts->type == FIELD_TYPE_UNSIGNED ||
	     def->cmp_def->parts->type == FIELD_TYPE_INTEGER))
		return metmx_hint_only_tree_index_new(memtx, def);
	return metmx_hinted_tree_index_new(memtx, def);
}
