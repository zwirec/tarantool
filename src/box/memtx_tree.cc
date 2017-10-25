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
#include "space.h"
#include "schema.h" /* space_cache_find() */
#include "errinj.h"
#include "memory.h"
#include "fiber.h"
#include <third_party/qsort_arg.h>

/* {{{ Utilities. *************************************************/

template <int Type>
static int
memtx_tree_qcompare(const void* a, const void *b, void *c)
{
	return (*(memtx_tree_data<Type> *) a).compare(
		*(memtx_tree_data<Type> *) b,
		(struct key_def *) c);
}

/* {{{ MemtxTree Iterators ****************************************/
template <int Type>
struct tree_iterator {
	struct iterator base;
	const class treeProxy<Type> *tree;
	struct index_def *index_def;
	typename treeProxy<Type>::iterator tree_iterator;
	enum iterator_type type;
	class memtx_tree_key_data<Type> key_data;
	class memtx_tree_data<Type> current;
};

template <int Type>
static void
tree_iterator_free(struct iterator *iterator);

template <int Type>
static inline struct tree_iterator<Type> *
cast_iterator(struct iterator *it)
{
	assert(it->free == &tree_iterator_free<Type>);
	return (struct tree_iterator<Type> *) it;
}

template <int Type>
static void
tree_iterator_free(struct iterator *iterator)
{
	struct tree_iterator<Type> *it = cast_iterator<Type>(iterator);
	if (it->current.tuple != NULL)
		tuple_unref(it->current.tuple);
	free(iterator);
}

static struct tuple *
tree_iterator_dummie(struct iterator *iterator)
{
	(void)iterator;
	return 0;
}

template <int Type>
static struct tuple *
tree_iterator_next(struct iterator *iterator)
{
	struct tree_iterator<Type> *it = cast_iterator<Type>(iterator);
	assert(it->current.tuple != NULL);
	memtx_tree_data<Type> *check = it->tree->get(it->tree_iterator);
	if (check == NULL || *check != it->current)
		it->tree_iterator = it->tree->upperBound(it->current, NULL);
	else
		it->tree->next(it->tree_iterator);
	tuple_unref(it->current.tuple);
	it->current.tuple = NULL;
	memtx_tree_data<Type> *res = it->tree->get(it->tree_iterator);
	if (res == NULL) {
		iterator->next = tree_iterator_dummie;
		return NULL;
	}
	it->current = *res;
	tuple_ref(it->current.tuple);
	return res->tuple;
}

template <int Type>
static struct tuple *
tree_iterator_prev(struct iterator *iterator)
{
	struct tree_iterator<Type> *it = cast_iterator<Type>(iterator);
	assert(it->current.tuple != NULL);
	memtx_tree_data<Type> *check = it->tree->get(it->tree_iterator);
	if (check == NULL || *check != it->current)
		it->tree_iterator = it->tree->lowerBound(it->current, NULL);
	it->tree->prev(it->tree_iterator);
	tuple_unref(it->current.tuple);
	it->current.tuple = NULL;
	memtx_tree_data<Type> *res = it->tree->get(it->tree_iterator);
	if (res == NULL) {
		iterator->next = tree_iterator_dummie;
		return NULL;
	}
	it->current = *res;
	tuple_ref(it->current.tuple);
	return res->tuple;
}

template <int Type>
static struct tuple *
tree_iterator_next_equal(struct iterator *iterator)
{
	struct tree_iterator<Type> *it = cast_iterator<Type>(iterator);
	assert(it->current.tuple != NULL);
	memtx_tree_data<Type> *check = it->tree->get(it->tree_iterator);
	if (check == NULL || *check != it->current)
		it->tree_iterator = it->tree->upperBound(it->current, NULL);
	else
		it->tree->next(it->tree_iterator);
	tuple_unref(it->current.tuple);
	it->current.tuple = NULL;
	memtx_tree_data<Type> *res = it->tree->get(it->tree_iterator);
	/* Use user key def to save a few loops. */
	if (!res || res->compare(&it->key_data, it->index_def->key_def) != 0) {
		iterator->next = tree_iterator_dummie;
		return NULL;
	}
	it->current = *res;
	tuple_ref(it->current.tuple);
	return res->tuple;
}

template <int Type>
static struct tuple *
tree_iterator_prev_equal(struct iterator *iterator)
{
	struct tree_iterator<Type> *it = cast_iterator<Type>(iterator);
	assert(it->current.tuple != NULL);
	memtx_tree_data<Type> *check = it->tree->get(it->tree_iterator);
	if (check == NULL || *check != it->current)
		it->tree_iterator = it->tree->lowerBound(it->current, NULL);
	it->tree->prev(it->tree_iterator);
	tuple_unref(it->current.tuple);
	it->current.tuple = NULL;
	memtx_tree_data<Type> *res = it->tree->get(it->tree_iterator);
	/* Use user key def to save a few loops. */
	if (!res || res->compare(&it->key_data, it->index_def->key_def) != 0) {
		iterator->next = tree_iterator_dummie;
		return NULL;
	}
	it->current = *res;
	tuple_ref(it->current.tuple);
	return res->tuple;
}

template <int Type>
static void
tree_iterator_set_next_method(struct tree_iterator<Type> *it)
{
	assert(it->current.tuple != NULL);
	switch (it->type) {
	case ITER_EQ:
		it->base.next = tree_iterator_next_equal<Type>;
		break;
	case ITER_REQ:
		it->base.next = tree_iterator_prev_equal<Type>;
		break;
	case ITER_ALL:
		it->base.next = tree_iterator_next<Type>;
		break;
	case ITER_LT:
	case ITER_LE:
		it->base.next = tree_iterator_prev<Type>;
		break;
	case ITER_GE:
	case ITER_GT:
		it->base.next = tree_iterator_next<Type>;
		break;
	default:
		/* The type was checked in initIterator */
		assert(false);
	}
}

template <int Type>
static struct tuple *
tree_iterator_start(struct iterator *iterator)
{
	struct tree_iterator<Type> *it = cast_iterator<Type>(iterator);
	it->base.next = tree_iterator_dummie;
	enum iterator_type type = it->type;
	bool exact = false;
	assert(it->current.tuple == NULL);
	if (it->key_data.key == NULL) {
		if (iterator_type_is_reverse(it->type))
			it->tree_iterator = it->tree->last();
		else
			it->tree_iterator = it->tree->first();
	} else {
		if (type == ITER_ALL || type == ITER_EQ ||
		    type == ITER_GE || type == ITER_LT) {
			it->tree_iterator = it->tree->lowerBound(&it->key_data,
								 &exact);
			if (type == ITER_EQ && !exact)
				return NULL;
		} else { // ITER_GT, ITER_REQ, ITER_LE
			it->tree_iterator = it->tree->upperBound(&it->key_data,
								 &exact);
			if (type == ITER_REQ && !exact)
				return NULL;
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
			it->tree->prev(it->tree_iterator);
		}
	}

	memtx_tree_data<Type> *res = it->tree->get(it->tree_iterator);
	if (!res)
		return NULL;
	it->current = *res;
	tuple_ref(it->current.tuple);
	tree_iterator_set_next_method(it);
	return res->tuple;
}

/* }}} */

/* {{{ MemtxTree  **********************************************************/

template <int Type>
MemtxTree<Type>::MemtxTree(struct index_def *index_def_arg)
	: MemtxIndex(index_def_arg),
	build_array(0),
	build_array_size(0),
	build_array_alloc_size(0)
{
	memtx_index_arena_init();
	/**
	 * Use extended key def for non-unique and nullable
	 * indexes. Unique, but nullable, index can store
	 * multiple NULLs. To correctly compare these NULLs
	 * extended key def must be used. For details @sa
	 * tuple_compare.cc.
	 */
	if (index_def->opts.is_unique && !index_def->key_def->is_nullable)
		cmp_def = index_def->key_def;
	else
		cmp_def = index_def->cmp_def;
	tree.create(cmp_def, memtx_index_extent_alloc, memtx_index_extent_free);
}

template <int Type>
MemtxTree<Type>::~MemtxTree()
{
	tree.destroy();
	free(build_array);
}

template <int Type>
size_t
MemtxTree<Type>::size() const
{
	return tree.size();
}

template <int Type>
size_t
MemtxTree<Type>::bsize() const
{
	return tree.memUsed();
}

template <int Type>
struct tuple *
MemtxTree<Type>::random(uint32_t rnd) const
{
	memtx_tree_data<Type> *res = tree.random(rnd);
	return res ? res->tuple : 0;
}

template <int Type>
struct tuple *
MemtxTree<Type>::findByKey(const char *key, uint32_t part_count) const
{
	assert(index_def->opts.is_unique && part_count == index_def->key_def->part_count);

	struct memtx_tree_key_data<Type> key_data;
	key_data.set(key, part_count, cmp_def);
	memtx_tree_data<Type> *res = tree.find(&key_data);
	return res ? res->tuple : 0;
}

template <int Type>
struct tuple *
MemtxTree<Type>::replace(struct tuple *old_tuple, struct tuple *new_tuple,
		   enum dup_replace_mode mode)
{
	uint32_t errcode;

	if (new_tuple) {
		struct tuple *dup_tuple = NULL;

		/* Try to optimistically replace the new_tuple. */
		int tree_res = tree.insert(new_tuple, &dup_tuple);
		if (tree_res) {
			tnt_raise(OutOfMemory, MEMTX_EXTENT_SIZE,
				  "MemtxTree", "replace");
		}

		errcode = replace_check_dup(old_tuple, dup_tuple, mode);

		if (errcode) {
			tree.remove(new_tuple);
			if (dup_tuple)
				tree.insert(dup_tuple);
			struct space *sp = space_cache_find(index_def->space_id);
			tnt_raise(ClientError, errcode, index_name(this),
				  space_name(sp));
		}
		if (dup_tuple)
			return dup_tuple;
	}
	if (old_tuple) {
		tree.remove(old_tuple);
	}
	return old_tuple;
}

template <int Type>
struct iterator *
MemtxTree<Type>::allocIterator() const
{
	struct tree_iterator<Type> *it = (struct tree_iterator<Type> *)
			calloc(1, sizeof(*it));
	if (it == NULL) {
		tnt_raise(OutOfMemory, sizeof(struct tree_iterator<Type>),
			  "MemtxTree", "iterator");
	}

	it->index_def = index_def;
	it->tree = &tree;
	it->base.free = tree_iterator_free<Type>;
	it->current.tuple = NULL;
	it->tree_iterator = tree.invalid();
	return (struct iterator *) it;
}

template <int Type>
void
MemtxTree<Type>::initIterator(struct iterator *iterator, enum iterator_type type,
			const char *key, uint32_t part_count) const
{
	assert(part_count == 0 || key != NULL);
	struct tree_iterator<Type> *it = cast_iterator<Type>(iterator);

	if (type < 0 || type > ITER_GT) /* Unsupported type */
		return Index::initIterator(iterator, type, key, part_count);

	if (part_count == 0) {
		/*
		 * If no key is specified, downgrade equality
		 * iterators to a full range.
		 */
		type = iterator_type_is_reverse(type) ? ITER_LE : ITER_GE;
		key = NULL;
	}
	if (it->current.tuple != NULL) {
		/*
		 * Free possible leftover tuple if the iterator
		 * is reused.
		 */
		tuple_unref(it->current.tuple);
		it->current.tuple = NULL;
	}
	it->type = type;
	it->key_data.set(key,  part_count, cmp_def);
	it->base.next = tree_iterator_start<Type>;
	it->tree_iterator = tree.invalid();
}

template <int Type>
void
MemtxTree<Type>::beginBuild()
{
	assert(tree.size() == 0);
}

template <int Type>
void
MemtxTree<Type>::reserve(uint32_t size_hint)
{
	if (size_hint < build_array_alloc_size)
		return;
	memtx_tree_data<Type> *tmp = (memtx_tree_data<Type> *)
		realloc(build_array, size_hint * sizeof(*tmp));
	if (tmp == NULL)
		tnt_raise(OutOfMemory, size_hint * sizeof(*tmp),
			"MemtxTree", "reserve");
	build_array = tmp;
	build_array_alloc_size = size_hint;
}

template <int Type>
void
MemtxTree<Type>::buildNext(struct tuple *tuple)
{
	if (build_array == NULL) {
		build_array = (memtx_tree_data<Type> *)
			malloc(MEMTX_EXTENT_SIZE);
		if (build_array == NULL) {
			tnt_raise(OutOfMemory, MEMTX_EXTENT_SIZE,
				"MemtxTree", "buildNext");
		}
		build_array_alloc_size =
			MEMTX_EXTENT_SIZE / sizeof(build_array[0]);
	}
	assert(build_array_size <= build_array_alloc_size);
	if (build_array_size == build_array_alloc_size) {
		build_array_alloc_size = build_array_alloc_size +
					 build_array_alloc_size / 2;
		memtx_tree_data<Type> *tmp = (memtx_tree_data<Type> *)
			realloc(build_array, build_array_alloc_size *
				sizeof(*tmp));
		if (tmp == NULL) {
			tnt_raise(OutOfMemory, build_array_alloc_size *
				sizeof(*tmp), "MemtxTree", "buildNext");
		}
		build_array = tmp;
	}
	build_array[build_array_size++].set(tuple, cmp_def);
}

template <int Type>
void
MemtxTree<Type>::endBuild()
{
	/** Use extended key def only for non-unique indexes. */
	struct key_def *cmp_def = index_def->opts.is_unique ?
		index_def->key_def : index_def->cmp_def;
	qsort_arg(build_array, build_array_size,
		  sizeof(build_array[0]),
		  memtx_tree_qcompare<Type>, cmp_def);
	tree.build(build_array, build_array_size);

	free(build_array);
	build_array = 0;
	build_array_size = 0;
	build_array_alloc_size = 0;
}

template <int Type>
struct tree_snapshot_iterator {
	struct snapshot_iterator base;
	struct treeProxy<Type> *tree;
	typename treeProxy<Type>::iterator tree_iterator;
};

template <int Type>
static void
tree_snapshot_iterator_free(struct snapshot_iterator *iterator)
{
	assert(iterator->free == &tree_snapshot_iterator_free<Type>);
	struct tree_snapshot_iterator<Type> *it =
		(struct tree_snapshot_iterator<Type> *)iterator;
	it->tree->destroyIterator(it->tree_iterator);
	free(iterator);
}

template <int Type>
static const char *
tree_snapshot_iterator_next(struct snapshot_iterator *iterator, uint32_t *size)
{
	assert(iterator->free == &tree_snapshot_iterator_free<Type>);
	struct tree_snapshot_iterator<Type> *it =
		(struct tree_snapshot_iterator<Type> *)iterator;
	memtx_tree_data<Type> *res = it->tree->get(it->tree_iterator);
	if (res == NULL)
		return NULL;
	it->tree->next(it->tree_iterator);
	return tuple_data_range(res->tuple, size);
}

/**
 * Create an ALL iterator with personal read view so further
 * index modifications will not affect the iteration results.
 * Must be destroyed by iterator->free after usage.
 */
template <int Type>
struct snapshot_iterator *
MemtxTree<Type>::createSnapshotIterator()
{
	struct tree_snapshot_iterator<Type> *it =
		(struct tree_snapshot_iterator<Type> *)
			calloc(1, sizeof(*it));
	if (it == NULL)
		tnt_raise(OutOfMemory, sizeof(*it), "MemtxTree", "iterator");

	it->base.free = tree_snapshot_iterator_free<Type>;
	it->base.next = tree_snapshot_iterator_next<Type>;
	it->tree = &tree;
	it->tree_iterator = tree.first();
	tree.freezeIterator(it->tree_iterator);
	return (struct snapshot_iterator *) it;
}

class Index *
new_memtx_tree(struct index_def *def)
{
	if (!def->opts.hint)
		return new MemtxTree<MEMTX_TREE_TYPE_NORMAL>(def);
	if (def->cmp_def->part_count == 1 &&
	    (def->cmp_def->parts->type == FIELD_TYPE_UNSIGNED ||
	     def->cmp_def->parts->type == FIELD_TYPE_INTEGER))
		return new MemtxTree<MEMTX_TREE_TYPE_HINT_ONLY>(def);
	return new MemtxTree<MEMTX_TREE_TYPE_HINTED>(def);
}
