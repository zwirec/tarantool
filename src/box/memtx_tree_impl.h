/*
 * Copyright 2010-2019 Tarantool AUTHORS: please see AUTHORS file.
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
#include "space.h"
#include "schema.h" /* space_cache_find() */
#include "errinj.h"
#include "memory.h"
#include "fiber.h"
#include "tuple.h"
#include <third_party/qsort_arg.h>
#include <small/mempool.h>

#ifndef memtx_tree_elem
#error "memtx_tree_elem must be defined"
#endif

#ifndef memtx_tree_key
#error "memtx_tree_key must be defined"
#endif

#define BPS_TREE_NAME CONCAT3(MEMTX_TREE_NAME, _, tree)
#define BPS_TREE_BLOCK_SIZE (512)
#define BPS_TREE_EXTENT_SIZE MEMTX_EXTENT_SIZE
#define BPS_TREE_EQUAL(a, b) MEMTX_TREE_EQUAL(a, b)
#define BPS_TREE_COMPARE(a, b, arg) MEMTX_TREE_COMPARE(a, b, arg)
#define BPS_TREE_COMPARE_KEY(a, b, arg) MEMTX_TREE_COMPARE_KEY(a, b, arg)
#define bps_tree_elem_t memtx_tree_elem
#define bps_tree_key_t memtx_tree_key *
#define bps_tree_arg_t struct key_def *

#include "salad/bps_tree.h"

#define bps_tree CONCAT3(MEMTX_TREE_NAME, _, tree)
#define _tree_api_name(postfix) CONCAT5(MEMTX_TREE_NAME, _, tree, _, postfix)
#define bps_tree_iterator _tree_api_name(iterator)

#define bps_tree_create _tree_api_name(create)
#define bps_tree_build _tree_api_name(build)
#define bps_tree_destroy _tree_api_name(destroy)
#define bps_tree_find _tree_api_name(find)
#define bps_tree_insert _tree_api_name(insert)
#define bps_tree_delete _tree_api_name(delete)
#define bps_tree_size _tree_api_name(size)
#define bps_tree_mem_used _tree_api_name(mem_used)
#define bps_tree_random _tree_api_name(random)
#define bps_tree_invalid_iterator _tree_api_name(invalid_iterator)
#define bps_tree_iterator_is_invalid _tree_api_name(iterator_is_invalid)
#define bps_tree_iterator_first _tree_api_name(iterator_first)
#define bps_tree_iterator_last _tree_api_name(iterator_last)
#define bps_tree_lower_bound _tree_api_name(lower_bound)
#define bps_tree_upper_bound _tree_api_name(upper_bound)
#define bps_tree_lower_bound_elem _tree_api_name(lower_bound_elem)
#define bps_tree_upper_bound_elem _tree_api_name(upper_bound_elem)
#define bps_tree_iterator_get_elem _tree_api_name(iterator_get_elem)
#define bps_tree_iterator_next _tree_api_name(iterator_next)
#define bps_tree_iterator_prev _tree_api_name(iterator_prev)
#define bps_tree_iterator_freeze _tree_api_name(iterator_freeze)
#define bps_tree_iterator_destroy _tree_api_name(iterator_destroy)

#undef _api_name
#define _api_name(postfix) CONCAT3(MEMTX_TREE_NAME, _, postfix)
#define memtx_tree_index _api_name(index)
#define memtx_tree_qcompare _api_name(qcompare)
#define memtx_tree_iterator _api_name(iterator)
#define memtx_tree_iterator_free _api_name(iterator_free)
#define memtx_tree_iterator_cast _api_name(iterator_cast)
#define memtx_tree_key_data_cast _api_name(key_data_cast)
#define memtx_tree_data_cast _api_name(data_cast)
#define memtx_tree_iterator_free _api_name(iterator_free)
#define memtx_tree_iterator_dummie _api_name(iterator_dummie)
#define memtx_tree_iterator_next _api_name(iterator_next)
#define memtx_tree_iterator_prev _api_name(iterator_prev)
#define memtx_tree_iterator_next_equal _api_name(iterator_next_equal)
#define memtx_tree_iterator_prev_equal _api_name(iterator_prev_equal)
#define memtx_tree_iterator_set_next_method _api_name(iterator_set_next_method)
#define memtx_tree_index_cmp_def _api_name(index_cmp_def)
#define memtx_tree_iterator_start _api_name(iterator_start)
#define memtx_tree_index_free _api_name(index_free)
#define memtx_tree_index_gc_run _api_name(index_gc_run)
#define memtx_tree_index_gc_free _api_name(index_gc_free)
#define memtx_tree_index_gc_vtab _api_name(index_gc_vtab)
#define memtx_tree_index_destroy _api_name(index_destroy)
#define memtx_tree_index_update_def _api_name(index_update_def)
#define memtx_tree_index_depends_on_pk _api_name(index_depends_on_pk)
#define memtx_tree_index_size _api_name(index_size)
#define memtx_tree_index_bsize _api_name(index_bsize)
#define memtx_tree_index_random _api_name(index_random)
#define memtx_tree_index_count _api_name(index_count)
#define memtx_tree_index_get _api_name(index_get)
#define memtx_tree_index_create_iterator _api_name(index_create_iterator)
#define memtx_tree_index_begin_build _api_name(index_begin_build)
#define memtx_tree_index_reserve _api_name(index_reserve)
#define memtx_tree_index_end_build _api_name(index_end_build)
#define memtx_tree_snapshot_iterator _api_name(snapshot_iterator)
#define memtx_tree_snapshot_iterator_free _api_name(snapshot_iterator_free)
#define memtx_tree_snapshot_iterator_next _api_name(snapshot_iterator_next)
#define memtx_tree_index_create_snapshot_iterator _api_name(index_create_snapshot_iterator)
#define memtx_tree_index_vtab _api_name(index_vtab)
#define memtx_tree_index_new _api_name(index_new)

struct memtx_tree_index {
	struct index base;
	memtx_tree_elem *build_array;
	size_t build_array_size, build_array_alloc_size;
	struct memtx_gc_task gc_task;
	struct bps_tree tree;
	struct bps_tree_iterator gc_iterator;
};

/* {{{ Utilities. *************************************************/

static int
memtx_tree_qcompare(const void* a, const void *b, void *c)
{
	return MEMTX_TREE_COMPARE(*(memtx_tree_elem *)a,
				*(memtx_tree_elem *)b, c);
}

/* {{{ MemtxTree Iterators ****************************************/
struct memtx_tree_iterator {
	struct iterator base;
	const struct bps_tree *tree;
	struct index_def *index_def;
	struct bps_tree_iterator tree_iterator;
	enum iterator_type type;
	memtx_tree_key key_data;
	memtx_tree_elem current;
	/** Memory pool the iterator was allocated from. */
	struct mempool *pool;
};

static void
memtx_tree_iterator_free(struct iterator *iterator);

static struct memtx_tree_iterator *
memtx_tree_iterator_cast(struct iterator *it)
{
	assert(it->free == memtx_tree_iterator_free);
	return (struct memtx_tree_iterator *) it;
}

struct memtx_tree_key_data *
memtx_tree_key_data_cast(memtx_tree_key *key)
{
	return (struct memtx_tree_key_data *)key;
}

struct memtx_tree_data *
memtx_tree_data_cast(memtx_tree_elem *data)
{
	return (struct memtx_tree_data *)data;
}

static void
memtx_tree_iterator_free(struct iterator *iterator)
{
	struct memtx_tree_iterator *it = memtx_tree_iterator_cast(iterator);
	if (memtx_tree_data_cast(&it->current)->tuple != NULL)
		tuple_unref(memtx_tree_data_cast(&it->current)->tuple);
	mempool_free(it->pool, it);
}

static int
memtx_tree_iterator_dummie(struct iterator *iterator, struct tuple **ret)
{
	(void)iterator;
	*ret = NULL;
	return 0;
}

static int
memtx_tree_iterator_next(struct iterator *iterator, struct tuple **ret)
{
	struct memtx_tree_iterator *it = memtx_tree_iterator_cast(iterator);
	assert(memtx_tree_data_cast(&it->current)->tuple != NULL);
	memtx_tree_elem *check =
		bps_tree_iterator_get_elem(it->tree, &it->tree_iterator);
	if (check == NULL || !MEMTX_TREE_EQUAL(*check, it->current)) {
		it->tree_iterator = bps_tree_upper_bound_elem(it->tree,
					it->current, NULL);
	} else {
		bps_tree_iterator_next(it->tree, &it->tree_iterator);
	}
	tuple_unref(memtx_tree_data_cast(&it->current)->tuple);
	memtx_tree_data_cast(&it->current)->tuple = NULL;
	memtx_tree_elem *res =
		bps_tree_iterator_get_elem(it->tree, &it->tree_iterator);
	if (res == NULL) {
		iterator->next = memtx_tree_iterator_dummie;
		*ret = NULL;
	} else {
		*ret = memtx_tree_data_cast(&it->current)->tuple =
		       memtx_tree_data_cast(res)->tuple;
		tuple_ref(memtx_tree_data_cast(&it->current)->tuple);
	}
	return 0;
}

static int
memtx_tree_iterator_prev(struct iterator *iterator, struct tuple **ret)
{
	struct memtx_tree_iterator *it = memtx_tree_iterator_cast(iterator);
	assert(memtx_tree_data_cast(&it->current)->tuple != NULL);
	memtx_tree_elem *check =
		bps_tree_iterator_get_elem(it->tree, &it->tree_iterator);
	if (check == NULL || !MEMTX_TREE_EQUAL(*check, it->current)) {
		it->tree_iterator = bps_tree_lower_bound_elem(it->tree,
					it->current, NULL);
	}
	bps_tree_iterator_prev(it->tree, &it->tree_iterator);
	tuple_unref(memtx_tree_data_cast(&it->current)->tuple);
	memtx_tree_data_cast(&it->current)->tuple = NULL;
	memtx_tree_elem *res =
		bps_tree_iterator_get_elem(it->tree, &it->tree_iterator);
	if (!res) {
		iterator->next = memtx_tree_iterator_dummie;
		*ret = NULL;
	} else {
		*ret = memtx_tree_data_cast(&it->current)->tuple =
		       memtx_tree_data_cast(res)->tuple;
		tuple_ref(memtx_tree_data_cast(&it->current)->tuple);
	}
	return 0;
}

static int
memtx_tree_iterator_next_equal(struct iterator *iterator, struct tuple **ret)
{
	struct memtx_tree_iterator *it = memtx_tree_iterator_cast(iterator);
	assert(memtx_tree_data_cast(&it->current)->tuple != NULL);
	memtx_tree_elem *check =
		bps_tree_iterator_get_elem(it->tree, &it->tree_iterator);
	if (check == NULL || !MEMTX_TREE_EQUAL(*check, it->current)) {
		it->tree_iterator = bps_tree_upper_bound_elem(it->tree,
					it->current, NULL);
	} else {
		bps_tree_iterator_next(it->tree, &it->tree_iterator);
	}
	tuple_unref(memtx_tree_data_cast(&it->current)->tuple);
	memtx_tree_data_cast(&it->current)->tuple = NULL;
	memtx_tree_elem *res =
		bps_tree_iterator_get_elem(it->tree, &it->tree_iterator);
	/* Use user key def to save a few loops. */
	if (!res || MEMTX_TREE_COMPARE_KEY(*res, &it->key_data,
					 it->index_def->key_def) != 0) {
		iterator->next = memtx_tree_iterator_dummie;
		*ret = NULL;
	} else {
		*ret = memtx_tree_data_cast(&it->current)->tuple =
		       memtx_tree_data_cast(res)->tuple;
		tuple_ref(memtx_tree_data_cast(&it->current)->tuple);
	}
	return 0;
}

static int
memtx_tree_iterator_prev_equal(struct iterator *iterator, struct tuple **ret)
{
	struct memtx_tree_iterator *it = memtx_tree_iterator_cast(iterator);
	assert(memtx_tree_data_cast(&it->current)->tuple != NULL);
	memtx_tree_elem *check =
		bps_tree_iterator_get_elem(it->tree, &it->tree_iterator);
	if (check == NULL || !MEMTX_TREE_EQUAL(*check, it->current)) {
		it->tree_iterator = bps_tree_lower_bound_elem(it->tree,
					it->current, NULL);
	}
	bps_tree_iterator_prev(it->tree, &it->tree_iterator);
	tuple_unref(memtx_tree_data_cast(&it->current)->tuple);
	memtx_tree_data_cast(&it->current)->tuple = NULL;
	memtx_tree_elem *res =
		bps_tree_iterator_get_elem(it->tree, &it->tree_iterator);
	/* Use user key def to save a few loops. */
	if (!res || MEMTX_TREE_COMPARE_KEY(*res, &it->key_data,
			it->index_def->key_def) != 0) {
		iterator->next = memtx_tree_iterator_dummie;
		*ret = NULL;
	} else {
		*ret = memtx_tree_data_cast(&it->current)->tuple =
		       memtx_tree_data_cast(res)->tuple;
		tuple_ref(memtx_tree_data_cast(&it->current)->tuple);
	}
	return 0;
}

static void
memtx_tree_iterator_set_next_method(struct memtx_tree_iterator *it)
{
	assert(memtx_tree_data_cast(&it->current)->tuple != NULL);
	switch (it->type) {
	case ITER_EQ:
		it->base.next = memtx_tree_iterator_next_equal;
		break;
	case ITER_REQ:
		it->base.next = memtx_tree_iterator_prev_equal;
		break;
	case ITER_ALL:
		it->base.next = memtx_tree_iterator_next;
		break;
	case ITER_LT:
	case ITER_LE:
		it->base.next = memtx_tree_iterator_prev;
		break;
	case ITER_GE:
	case ITER_GT:
		it->base.next = memtx_tree_iterator_next;
		break;
	default:
		/* The type was checked in initIterator */
		assert(false);
	}
}

static int
memtx_tree_iterator_start(struct iterator *iterator, struct tuple **ret)
{
	*ret = NULL;
	struct memtx_tree_iterator *it = memtx_tree_iterator_cast(iterator);
	it->base.next = memtx_tree_iterator_dummie;
	const struct bps_tree *tree = it->tree;
	enum iterator_type type = it->type;
	bool exact = false;
	assert(memtx_tree_data_cast(&it->current)->tuple == NULL);
	if (memtx_tree_key_data_cast(&it->key_data)->key == NULL) {
		if (iterator_type_is_reverse(it->type))
			it->tree_iterator = bps_tree_iterator_last(tree);
		else
			it->tree_iterator = bps_tree_iterator_first(tree);
	} else {
		if (type == ITER_ALL || type == ITER_EQ ||
		    type == ITER_GE || type == ITER_LT) {
			it->tree_iterator = bps_tree_lower_bound(tree,
						&it->key_data, &exact);
			if (type == ITER_EQ && !exact)
				return 0;
		} else { // ITER_GT, ITER_REQ, ITER_LE
			it->tree_iterator = bps_tree_upper_bound(tree,
						&it->key_data, &exact);
			if (type == ITER_REQ && !exact)
				return 0;
		}
		if (iterator_type_is_reverse(type)) {
			/*
			 * Because of limitations of tree search API we use use
			 * lower_bound for LT search and upper_bound for LE
			 * and REQ searches. Thus we found position to the
			 * right of the target one. Let's make a step to the
			 * left to reach target position.
			 * If we found an invalid iterator all the elements in
			 * the tree are less (less or equal) to the key, and
			 * iterator_next call will convert the iterator to the
			 * last position in the tree, that's what we need.
			 */
			bps_tree_iterator_prev(it->tree, &it->tree_iterator);
		}
	}

	memtx_tree_elem *res =
		bps_tree_iterator_get_elem(it->tree, &it->tree_iterator);
	if (!res)
		return 0;
	*ret = memtx_tree_data_cast(&it->current)->tuple =
	       memtx_tree_data_cast(res)->tuple;
	tuple_ref(memtx_tree_data_cast(&it->current)->tuple);
	memtx_tree_iterator_set_next_method(it);
	return 0;
}

/* }}} */

/* {{{ MemtxTree  **********************************************************/

/**
 * Return the key def to use for comparing tuples stored
 * in the given tree index.
 *
 * We use extended key def for non-unique and nullable
 * indexes. Unique but nullable index can store multiple
 * NULLs. To correctly compare these NULLs extended key
 * def must be used. For details @sa tuple_compare.cc.
 */
static struct key_def *
memtx_tree_index_cmp_def(struct memtx_tree_index *index)
{
	struct index_def *def = index->base.def;
	return def->opts.is_unique && !def->key_def->is_nullable ?
		def->key_def : def->cmp_def;
}

static void
memtx_tree_index_free(struct memtx_tree_index *index)
{
	bps_tree_destroy(&index->tree);
	free(index->build_array);
	free(index);
}

static void
memtx_tree_index_gc_run(struct memtx_gc_task *task, bool *done)
{
	/*
	 * Yield every 1K tuples to keep latency < 0.1 ms.
	 * Yield more often in debug mode.
	 */
#ifdef NDEBUG
	enum { YIELD_LOOPS = 1000 };
#else
	enum { YIELD_LOOPS = 10 };
#endif

	struct memtx_tree_index *index = container_of(task,
			struct memtx_tree_index, gc_task);
	struct bps_tree *tree = &index->tree;
	struct bps_tree_iterator *itr = &index->gc_iterator;

	unsigned int loops = 0;
	while (!bps_tree_iterator_is_invalid(itr)) {
		memtx_tree_elem *res =
			bps_tree_iterator_get_elem(tree, itr);
		struct tuple *tuple = memtx_tree_data_cast(res)->tuple;;
		bps_tree_iterator_next(tree, itr);
		tuple_unref(tuple);
		if (++loops >= YIELD_LOOPS) {
			*done = false;
			return;
		}
	}
	*done = true;
}

static void
memtx_tree_index_gc_free(struct memtx_gc_task *task)
{
	struct memtx_tree_index *index = container_of(task,
			struct memtx_tree_index, gc_task);
	memtx_tree_index_free(index);
}

static const struct memtx_gc_task_vtab memtx_tree_index_gc_vtab = {
	.run = memtx_tree_index_gc_run,
	.free = memtx_tree_index_gc_free,
};

static void
memtx_tree_index_destroy(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	struct memtx_engine *memtx = (struct memtx_engine *)base->engine;
	if (base->def->iid == 0) {
		/*
		 * Primary index. We need to free all tuples stored
		 * in the index, which may take a while. Schedule a
		 * background task in order not to block tx thread.
		 */
		index->gc_task.vtab = &memtx_tree_index_gc_vtab;
		index->gc_iterator =
			bps_tree_iterator_first(&index->tree);
		memtx_engine_schedule_gc(memtx, &index->gc_task);
	} else {
		/*
		 * Secondary index. Destruction is fast, no need to
		 * hand over to background fiber.
		 */
		memtx_tree_index_free(index);
	}
}

static void
memtx_tree_index_update_def(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	index->tree.arg = memtx_tree_index_cmp_def(index);
}

static bool
memtx_tree_index_depends_on_pk(struct index *base)
{
	struct index_def *def = base->def;
	/* See comment to memtx_tree_index_cmp_def(). */
	return !def->opts.is_unique || def->key_def->is_nullable;
}

static ssize_t
memtx_tree_index_size(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	return bps_tree_size(&index->tree);
}

static ssize_t
memtx_tree_index_bsize(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	return bps_tree_mem_used(&index->tree);
}

static int
memtx_tree_index_random(struct index *base, uint32_t rnd, struct tuple **result)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	memtx_tree_elem *res = bps_tree_random(&index->tree, rnd);
	*result = res != NULL ? memtx_tree_data_cast(res)->tuple : NULL;
	return 0;
}

static ssize_t
memtx_tree_index_count(struct index *base, enum iterator_type type,
		       const char *key, uint32_t part_count)
{
	if (type == ITER_ALL)
		return memtx_tree_index_size(base); /* optimization */
	return generic_index_count(base, type, key, part_count);
}

static int
memtx_tree_index_get(struct index *base, const char *key,
		     uint32_t part_count, struct tuple **result)
{
	assert(base->def->opts.is_unique &&
	       part_count == base->def->key_def->part_count);
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	memtx_tree_key key_data;
	struct key_def *cmp_def = memtx_tree_index_cmp_def(index);
	MEMTX_TREE_KEY_SET(&key_data, key, part_count, cmp_def);
	memtx_tree_elem *res = bps_tree_find(&index->tree, &key_data);
	*result = res != NULL ? memtx_tree_data_cast(res)->tuple : NULL;
	return 0;
}

#ifndef memtx_tree_index_insert_tuple
#define memtx_tree_index_insert_tuple _api_name(index_insert_tuple)
static int
memtx_tree_index_insert_tuple(struct index *base, struct tuple *tuple,
			      struct tuple **replaced)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	memtx_tree_elem data;
	MEMTX_TREE_ELEM_SET(&data, tuple, index->tree.arg);
	memtx_tree_elem data_replaced;
	memset(&data_replaced, 0, sizeof(data_replaced));
	int rc = bps_tree_insert(&index->tree, data, &data_replaced);
	if (replaced != NULL)
		*replaced = memtx_tree_data_cast(&data_replaced)->tuple;
	return rc;
}
#endif /* memtx_tree_index_insert_tuple */

#ifndef memtx_tree_index_delete_tuple
#define memtx_tree_index_delete_tuple _api_name(index_delete_tuple)
static void
memtx_tree_index_delete_tuple(struct index *base, struct tuple *tuple)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	memtx_tree_elem data;
	MEMTX_TREE_ELEM_SET(&data, tuple, index->tree.arg);
	bps_tree_delete(&index->tree, data);
}
#endif /* memtx_tree_index_delete_tuple */

#ifndef memtx_tree_index_replace
#define memtx_tree_index_replace _api_name(index_replace)
static int
memtx_tree_index_replace(struct index *base, struct tuple *old_tuple,
			 struct tuple *new_tuple, enum dup_replace_mode mode,
			 struct tuple **result)
{
	if (new_tuple) {
		struct tuple *dup_tuple = NULL;

		/* Try to optimistically replace the new_tuple. */
		int tree_res = memtx_tree_index_insert_tuple(base, new_tuple,
						 &dup_tuple);
		if (tree_res) {
			diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
				 "memtx_tree_index", "replace");
			return -1;
		}

		uint32_t errcode = replace_check_dup(old_tuple,
						     dup_tuple, mode);
		if (errcode) {
			memtx_tree_index_delete_tuple(base, new_tuple);
			if (dup_tuple)
				memtx_tree_index_insert_tuple(base, dup_tuple, 0);
			struct space *sp = space_cache_find(base->def->space_id);
			if (sp != NULL)
				diag_set(ClientError, errcode, base->def->name,
					 space_name(sp));
			return -1;
		}
		if (dup_tuple) {
			*result = dup_tuple;
			return 0;
		}
	}
	if (old_tuple)
		memtx_tree_index_delete_tuple(base, old_tuple);
	*result = old_tuple;
	return 0;
}
#endif /* memtx_tree_index_replace */

static struct iterator *
memtx_tree_index_create_iterator(struct index *base, enum iterator_type type,
				 const char *key, uint32_t part_count)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	struct memtx_engine *memtx = (struct memtx_engine *)base->engine;

	assert(part_count == 0 || key != NULL);
	if (type > ITER_GT) {
		diag_set(UnsupportedIndexFeature, base->def,
			 "requested iterator type");
		return NULL;
	}

	if (part_count == 0) {
		/*
		 * If no key is specified, downgrade equality
		 * iterators to a full range.
		 */
		type = iterator_type_is_reverse(type) ? ITER_LE : ITER_GE;
		key = NULL;
	}

	struct memtx_tree_iterator *it =
		mempool_alloc(&memtx->tree_iterator_pool);
	if (it == NULL) {
		diag_set(OutOfMemory, sizeof(struct memtx_tree_iterator),
			 "memtx_tree_index", "iterator");
		return NULL;
	}
	iterator_create(&it->base, base);
	it->pool = &memtx->tree_iterator_pool;
	it->base.next = memtx_tree_iterator_start;
	it->base.free = memtx_tree_iterator_free;
	it->type = type;
	struct key_def *cmp_def = memtx_tree_index_cmp_def(index);
	MEMTX_TREE_KEY_SET(&it->key_data, key, part_count, cmp_def);
	it->index_def = base->def;
	it->tree = &index->tree;
	it->tree_iterator = bps_tree_invalid_iterator();
	memtx_tree_data_cast(&it->current)->tuple = NULL;
	return (struct iterator *)it;
}

static void
memtx_tree_index_begin_build(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	assert(bps_tree_size(&index->tree) == 0);
	(void)index;
}

static int
memtx_tree_index_reserve(struct index *base, uint32_t size_hint)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	if (size_hint < index->build_array_alloc_size)
		return 0;
	memtx_tree_elem *tmp =
		realloc(index->build_array, size_hint * sizeof(*tmp));
	if (tmp == NULL) {
		diag_set(OutOfMemory, size_hint * sizeof(*tmp),
			 "memtx_tree_index", "reserve");
		return -1;
	}
	index->build_array = tmp;
	index->build_array_alloc_size = size_hint;
	return 0;
}

#ifndef memtx_tree_index_build_next
#define memtx_tree_index_build_next _api_name(index_build_next)
static int
memtx_tree_index_build_next(struct index *base, struct tuple *tuple)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	if (index->build_array == NULL) {
		index->build_array =
			(memtx_tree_elem *)malloc(MEMTX_EXTENT_SIZE);
		if (index->build_array == NULL) {
			diag_set(OutOfMemory, MEMTX_EXTENT_SIZE,
				 "memtx_tree_index", "build_next");
			return -1;
		}
		index->build_array_alloc_size =
			MEMTX_EXTENT_SIZE / sizeof(index->build_array[0]);
	}
	assert(index->build_array_size <= index->build_array_alloc_size);
	if (index->build_array_size == index->build_array_alloc_size) {
		index->build_array_alloc_size = index->build_array_alloc_size +
					index->build_array_alloc_size / 2;
		memtx_tree_elem *tmp = realloc(index->build_array,
				index->build_array_alloc_size * sizeof(*tmp));
		if (tmp == NULL) {
			diag_set(OutOfMemory, index->build_array_alloc_size *
				 sizeof(*tmp), "memtx_tree_index", "build_next");
			return -1;
		}
		index->build_array = tmp;
	}
	MEMTX_TREE_ELEM_SET(&index->build_array[index->build_array_size++],
			    tuple, memtx_tree_index_cmp_def(index));
	return 0;
}
#endif /* memtx_tree_index_build_next */

static void
memtx_tree_index_end_build(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	struct key_def *cmp_def = memtx_tree_index_cmp_def(index);
	qsort_arg(index->build_array, index->build_array_size,
		  sizeof(index->build_array[0]), memtx_tree_qcompare, cmp_def);
	bps_tree_build(&index->tree, index->build_array,
		       index->build_array_size);

	free(index->build_array);
	index->build_array = NULL;
	index->build_array_size = 0;
	index->build_array_alloc_size = 0;
}

struct memtx_tree_snapshot_iterator {
	struct snapshot_iterator base;
	struct bps_tree *tree;
	struct bps_tree_iterator tree_iterator;
};

static void
memtx_tree_snapshot_iterator_free(struct snapshot_iterator *iterator)
{
	assert(iterator->free == memtx_tree_snapshot_iterator_free);
	struct memtx_tree_snapshot_iterator *it =
		(struct memtx_tree_snapshot_iterator *)iterator;
	bps_tree_iterator_destroy(it->tree, &it->tree_iterator);
	free(iterator);
}

static const char *
memtx_tree_snapshot_iterator_next(struct snapshot_iterator *iterator, uint32_t *size)
{
	assert(iterator->free == memtx_tree_snapshot_iterator_free);
	struct memtx_tree_snapshot_iterator *it =
		(struct memtx_tree_snapshot_iterator *)iterator;
	memtx_tree_elem *res =
		bps_tree_iterator_get_elem(it->tree, &it->tree_iterator);
	if (res == NULL)
		return NULL;
	bps_tree_iterator_next(it->tree, &it->tree_iterator);
	return tuple_data_range(memtx_tree_data_cast(res)->tuple, size);
}

/**
 * Create an ALL iterator with personal read view so further
 * index modifications will not affect the iteration results.
 * Must be destroyed by iterator->free after usage.
 */
static struct snapshot_iterator *
memtx_tree_index_create_snapshot_iterator(struct index *base)
{
	struct memtx_tree_index *index = (struct memtx_tree_index *)base;
	struct memtx_tree_snapshot_iterator *it = (struct memtx_tree_snapshot_iterator *)
		calloc(1, sizeof(*it));
	if (it == NULL) {
		diag_set(OutOfMemory, sizeof(struct memtx_tree_snapshot_iterator),
			 "memtx_tree_index", "create_snapshot_iterator");
		return NULL;
	}

	it->base.free = memtx_tree_snapshot_iterator_free;
	it->base.next = memtx_tree_snapshot_iterator_next;
	it->tree = &index->tree;
	it->tree_iterator = bps_tree_iterator_first(&index->tree);
	bps_tree_iterator_freeze(&index->tree, &it->tree_iterator);
	return (struct snapshot_iterator *) it;
}

static const struct index_vtab memtx_tree_index_vtab = {
	/* .destroy = */ memtx_tree_index_destroy,
	/* .commit_create = */ generic_index_commit_create,
	/* .abort_create = */ generic_index_abort_create,
	/* .commit_modify = */ generic_index_commit_modify,
	/* .commit_drop = */ generic_index_commit_drop,
	/* .update_def = */ memtx_tree_index_update_def,
	/* .depends_on_pk = */ memtx_tree_index_depends_on_pk,
	/* .def_change_requires_rebuild = */
		memtx_index_def_change_requires_rebuild,
	/* .size = */ memtx_tree_index_size,
	/* .bsize = */ memtx_tree_index_bsize,
	/* .min = */ generic_index_min,
	/* .max = */ generic_index_max,
	/* .random = */ memtx_tree_index_random,
	/* .count = */ memtx_tree_index_count,
	/* .get = */ memtx_tree_index_get,
	/* .replace = */ memtx_tree_index_replace,
	/* .create_iterator = */ memtx_tree_index_create_iterator,
	/* .create_snapshot_iterator = */
		memtx_tree_index_create_snapshot_iterator,
	/* .stat = */ generic_index_stat,
	/* .compact = */ generic_index_compact,
	/* .reset_stat = */ generic_index_reset_stat,
	/* .begin_build = */ memtx_tree_index_begin_build,
	/* .reserve = */ memtx_tree_index_reserve,
	/* .build_next = */ memtx_tree_index_build_next,
	/* .end_build = */ memtx_tree_index_end_build,
};

struct index *
memtx_tree_index_new(struct memtx_engine *memtx, struct index_def *def)
{
	if (!mempool_is_initialized(&memtx->tree_iterator_pool)) {
		mempool_create(&memtx->tree_iterator_pool, cord_slab_cache(),
			       sizeof(struct memtx_tree_iterator));
	}

	struct memtx_tree_index *index =
		(struct memtx_tree_index *)calloc(1, sizeof(*index));
	if (index == NULL) {
		diag_set(OutOfMemory, sizeof(*index),
			 "malloc", "struct memtx_tree_index");
		return NULL;
	}
	if (index_create(&index->base, (struct engine *)memtx,
			 &memtx_tree_index_vtab, def) != 0) {
		free(index);
		return NULL;
	}

	struct key_def *cmp_def = memtx_tree_index_cmp_def(index);
	bps_tree_create(&index->tree, cmp_def, memtx_index_extent_alloc,
			memtx_index_extent_free, memtx);
	return (struct index *)index;
}

#undef _api_name
#undef memtx_tree_index
#undef memtx_tree_qcompare
#undef memtx_tree_iterator
#undef memtx_tree_iterator_free
#undef memtx_tree_iterator_cast
#undef memtx_tree_key_data
#undef memtx_tree_data
#undef memtx_tree_iterator_free
#undef memtx_tree_iterator_dummie
#undef memtx_tree_iterator_next
#undef memtx_tree_iterator_prev
#undef memtx_tree_iterator_next_equal
#undef memtx_tree_iterator_prev_equal
#undef memtx_tree_iterator_set_next_method
#undef memtx_tree_index_cmp_def
#undef memtx_tree_index_free
#undef memtx_tree_index_gc_run
#undef memtx_tree_index_gc_free
#undef memtx_tree_index_gc_vtab
#undef memtx_tree_index_destroy
#undef memtx_tree_index_update_def
#undef memtx_tree_index_depends_on_pk
#undef memtx_tree_index_size
#undef memtx_tree_index_bsize
#undef memtx_tree_index_random
#undef memtx_tree_index_count
#undef memtx_tree_index_get
#undef memtx_tree_index_insert_tuple
#undef memtx_tree_index_delete_tuple
#undef memtx_tree_index_replace
#undef memtx_tree_index_create_iterator
#undef memtx_tree_index_begin_build
#undef memtx_tree_index_reserve
#undef memtx_tree_index_build_next
#undef memtx_tree_index_end_build
#undef memtx_tree_snapshot_iterator
#undef memtx_tree_snapshot_iterator_free
#undef memtx_tree_snapshot_iterator_next
#undef memtx_tree_index_create_snapshot_iterator
#undef memtx_tree_index_vtab
#undef memtx_tree_index_new

#undef _tree_api_name

#undef bps_tree
#undef bps_tree_iterator
#undef bps_tree_create
#undef bps_tree_build
#undef bps_tree_destroy
#undef bps_tree_find
#undef bps_tree_insert
#undef bps_tree_delete
#undef bps_tree_size
#undef bps_tree_mem_used
#undef bps_tree_random
#undef bps_tree_invalid_iterator
#undef bps_tree_iterator_is_invalid
#undef bps_tree_iterator_first
#undef bps_tree_iterator_last
#undef bps_tree_lower_bound
#undef bps_tree_upper_bound
#undef bps_tree_lower_bound_elem
#undef bps_tree_upper_bound_elem
#undef bps_tree_iterator_get_elem
#undef bps_tree_iterator_next
#undef bps_tree_iterator_prev
#undef bps_tree_iterator_freeze
#undef bps_tree_iterator_destroy

#undef BPS_TREE_NAME
#undef BPS_TREE_BLOCK_SIZE
#undef BPS_TREE_EXTENT_SIZE
#undef BPS_TREE_EQUAL
#undef BPS_TREE_COMPARE
#undef BPS_TREE_COMPARE_KEY
#undef bps_tree_elem_t
#undef bps_tree_key_t
#undef bps_tree_arg_t
