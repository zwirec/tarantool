#ifndef TARANTOOL_BOX_MEMTX_TREE_PROXY_H_INCLUDED
#define TARANTOOL_BOX_MEMTX_TREE_PROXY_H_INCLUDED
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

#include "tuple_compare.h"

enum {
	MEMTX_TREE_TYPE_NORMAL = 0,
	MEMTX_TREE_TYPE_HINTED = 1,
	MEMTX_TREE_TYPE_HINT_ONLY = 2,
};

template <int Type> struct memtx_tree_key_data;

/**
 * Struct that is used as a key in BPS tree definition.
 */
template <>
struct memtx_tree_key_data<MEMTX_TREE_TYPE_NORMAL> {
	/**
	 * Sequence of msgpacked search fields.
	 * Use 'set' method to set the field!
	 */
	const char *key;
	/*
	 * Number of msgpacked search fields
	 * Use 'set' method to set the field!
	 */
	uint32_t part_count;

	/** Set key and part count and recalculate internal depending fields */
	void
	set(const char *_key, uint32_t _part_count, const struct key_def *def)
	{
		key = _key;
		part_count = _part_count;
		(void)def;
	}
};

template<>
struct memtx_tree_key_data<MEMTX_TREE_TYPE_HINTED> {
	/**
	 * Compare hint.
	 * Is calculated automatically in 'set' method.
	 */
	uint64_t hint;
	/**
	 * Sequence of msgpacked search fields.
	 * Use 'set' method to set the field!
	 */
	const char *key;
	/*
	 * Number of msgpacked search fields
	 * Use 'set' method to set the field!
	 */
	uint32_t part_count;

	/** Set key and part count and recalculate internal depending fields */
	void
	set(const char *_key, uint32_t _part_count, const struct key_def *def)
	{
		key = _key;
		part_count = _part_count;
		if (part_count > 0)
			hint = key_hint(key, def);
		else
			hint = 0;
	}

};

template<>
struct memtx_tree_key_data<MEMTX_TREE_TYPE_HINT_ONLY> {
	/**
	 * Compare hint.
	 * Is calculated automatically in 'set' method.
	 */
	uint64_t hint;
	/**
	 * Sequence of msgpacked search fields.
	 * Use 'set' method to set the field!
	 */
	const char *key;
	/*
	 * Number of msgpacked search fields
	 * Use 'set' method to set the field!
	 */
	uint32_t part_count;

	/** Set key and part count and recalculate internal depending fields */
	void
	set(const char *_key, uint32_t _part_count, const struct key_def *def)
	{
		key = _key;
		part_count = _part_count;
		if (part_count > 0)
			hint = key_hint(key, def);
		else
			hint = 0;
	}

};

template <int Type> struct memtx_tree_data;

/**
 * Struct that is used as an unit of storage in BPS tree.
 */
template <>
struct memtx_tree_data<MEMTX_TREE_TYPE_NORMAL> {
	/** Constant for general/partial specialization compatibility */
	enum { Type = MEMTX_TREE_TYPE_NORMAL };
	/**
	 * Storing tuple.
	 * Use constructor or 'set' method to set the field!
	 */
	struct tuple *tuple;

	memtx_tree_data() : tuple(NULL) {}
	memtx_tree_data(struct tuple *_tuple, const struct key_def *def)
	{
		set(_tuple, def);
	}
	/** Set tuple and recalculate internal depending fields */
	void
	set(struct tuple *_tuple, const struct key_def *def)
	{
		tuple = _tuple;
		(void)def;
	}
	bool
	operator != (const memtx_tree_data &a) const
	{
		return tuple != a.tuple;
	}
	int
	compare(const memtx_tree_data &a, struct key_def *def) const
	{
		return tuple_compare(tuple, a.tuple, def);
	}
	int
	compare(const memtx_tree_key_data<Type> *a, struct key_def *def) const
	{
		assert(a->part_count != 0);
		return tuple_compare_with_key(tuple, a->key, a->part_count, def);
	}

};

template <>
struct memtx_tree_data<MEMTX_TREE_TYPE_HINTED> {
	/** Constant for general/partial specialization compatibility */
	enum { Type = MEMTX_TREE_TYPE_HINTED };
	/**
	 * Compare hint.
	 * Is calculated automatically in 'set' method.
	 */
	uint64_t hint;
	/**
	 * Storing tuple.
	 * Use constructor or 'set' method to set the field!
	 */
	struct tuple *tuple;

	memtx_tree_data() : tuple(NULL) {}
	memtx_tree_data(struct tuple *_tuple, const struct key_def *def)
	{
		set(_tuple, def);
	}
	void
	set(struct tuple *_tuple, const struct key_def *def)
	{
		tuple = _tuple;
		hint = tuple_hint(tuple, def);
	}
	bool
	operator != (const memtx_tree_data &a) const
	{
		return tuple != a.tuple;
	}
	int
	compare(const memtx_tree_data &a, struct key_def *def) const
	{
		if (hint != a.hint) {
			return hint < a.hint ? -1 : 1;
		}
		return tuple_compare(tuple, a.tuple, def);
	}
	int
	compare(const memtx_tree_key_data<Type> *a, struct key_def *def) const
	{
		assert(a->part_count != 0);
		if (hint != a->hint)
			return hint < a->hint ? -1 : 1;
		return tuple_compare_with_key(tuple, a->key, a->part_count, def);
	}
};

template <>
struct memtx_tree_data<MEMTX_TREE_TYPE_HINT_ONLY> {
	/** Constant for general/partial specialization compatibility */
	enum { Type = MEMTX_TREE_TYPE_HINT_ONLY };
	/**
	 * Compare hint.
	 * Is calculated automatically in 'set' method.
	 */
	uint64_t hint;
	/**
	 * Storing tuple.
	 * Use constructor or 'set' method to set the field!
	 */
	struct tuple *tuple;

	memtx_tree_data() : tuple(NULL) {}
	memtx_tree_data(struct tuple *_tuple, const struct key_def *def)
	{
		set(_tuple, def);
	}
	void
	set(struct tuple *_tuple, const struct key_def *def)
	{
		tuple = _tuple;
		hint = tuple_hint(tuple, def);
	}
	bool
	operator != (const memtx_tree_data &a) const
	{
		return tuple != a.tuple;
	}
	int
	compare(const memtx_tree_data &a, struct key_def *def) const
	{
		(void)def;
		return hint < a.hint ? -1 : hint > a.hint;
	}
	int
	compare(const memtx_tree_key_data<Type> *a, struct key_def *def) const
	{
		(void)def;
		assert(a->part_count != 0);
		return hint < a->hint ? -1 : hint > a->hint;
	}
};

#define BPS_TREE_NAME memtx_tree_normal
#define BPS_TREE_BLOCK_SIZE (512)
#define BPS_TREE_EXTENT_SIZE MEMTX_EXTENT_SIZE
#define BPS_TREE_COMPARE(a, b, arg) (a).compare(b, arg)
#define BPS_TREE_COMPARE_KEY(a, b, arg) (a).compare(b, arg)
#define bps_tree_elem_t memtx_tree_data<MEMTX_TREE_TYPE_NORMAL>
#define bps_tree_key_t memtx_tree_key_data<MEMTX_TREE_TYPE_NORMAL> *
#define bps_tree_arg_t struct key_def *

#include "salad/bps_tree.h"

#undef BPS_TREE_NAME
#undef BPS_TREE_BLOCK_SIZE
#undef BPS_TREE_EXTENT_SIZE
#undef BPS_TREE_COMPARE
#undef BPS_TREE_COMPARE_KEY
#undef bps_tree_elem_t
#undef bps_tree_key_t
#undef bps_tree_arg_t

#define BPS_TREE_NAME memtx_tree_hinted
#define BPS_TREE_BLOCK_SIZE (512)
#define BPS_TREE_EXTENT_SIZE MEMTX_EXTENT_SIZE
#define BPS_TREE_COMPARE(a, b, arg) (a).compare(b, arg)
#define BPS_TREE_COMPARE_KEY(a, b, arg) (a).compare(b, arg)
#define bps_tree_elem_t memtx_tree_data<MEMTX_TREE_TYPE_HINTED>
#define bps_tree_key_t memtx_tree_key_data<MEMTX_TREE_TYPE_HINTED> *
#define bps_tree_arg_t struct key_def *

#include "salad/bps_tree.h"

#undef BPS_TREE_NAME
#undef BPS_TREE_BLOCK_SIZE
#undef BPS_TREE_EXTENT_SIZE
#undef BPS_TREE_COMPARE
#undef BPS_TREE_COMPARE_KEY
#undef bps_tree_elem_t
#undef bps_tree_key_t
#undef bps_tree_arg_t

#define BPS_TREE_NAME memtx_tree_hint_only
#define BPS_TREE_BLOCK_SIZE (512)
#define BPS_TREE_EXTENT_SIZE MEMTX_EXTENT_SIZE
#define BPS_TREE_COMPARE(a, b, arg) (a).compare(b, arg)
#define BPS_TREE_COMPARE_KEY(a, b, arg) (a).compare(b, arg)
#define bps_tree_elem_t memtx_tree_data<MEMTX_TREE_TYPE_HINT_ONLY>
#define bps_tree_key_t memtx_tree_key_data<MEMTX_TREE_TYPE_HINT_ONLY> *
#define bps_tree_arg_t struct key_def *
#define BPS_BLOCK_LINEAR_SEARCH 1

#include "salad/bps_tree.h"

#undef BPS_TREE_NAME
#undef BPS_TREE_BLOCK_SIZE
#undef BPS_TREE_EXTENT_SIZE
#undef BPS_TREE_COMPARE
#undef BPS_TREE_COMPARE_KEY
#undef bps_tree_elem_t
#undef bps_tree_key_t
#undef bps_tree_arg_t
#undef BPS_BLOCK_LINEAR_SEARCH

template <int Type> struct treeProxy;

template <>
class treeProxy<MEMTX_TREE_TYPE_NORMAL> {
public:
	/** Constant for general/partial specialization compatibility */
	enum { Type = MEMTX_TREE_TYPE_NORMAL };
	struct iterator	{
		memtx_tree_normal_iterator iterator;
	};
	void
	create(struct key_def *def,
	       bps_tree_extent_alloc_f alloc, bps_tree_extent_free_f free)
	{
		memtx_tree_normal_create(&tree, def, alloc, free, NULL);
	}
	void
	destroy()
	{
		memtx_tree_normal_destroy(&tree);
	}
	memtx_tree_data<Type> *
	get(struct iterator &it) const
	{
		return memtx_tree_normal_iterator_get_elem(&tree, &it.iterator);
	}
	memtx_tree_data<Type> *
	random(uint32_t seed) const
	{
		return memtx_tree_normal_random(&tree, seed);
	}
	memtx_tree_data<Type> *
	find(memtx_tree_key_data<Type> *key_data) const
	{
		return memtx_tree_normal_find(&tree, key_data);
	}
	iterator
	lowerBound(const memtx_tree_data<Type> &data, bool *exact) const
	{
		iterator result;
		result.iterator = memtx_tree_normal_lower_bound_elem(&tree, data, exact);
		return result;
	}
	iterator
	upperBound(const memtx_tree_data<Type> &data, bool *exact) const
	{
		iterator result;
		result.iterator = memtx_tree_normal_upper_bound_elem(&tree, data, exact);
		return result;
	}
	iterator
	lowerBound(memtx_tree_key_data<Type> *data, bool *exact) const
	{
		iterator result;
		result.iterator = memtx_tree_normal_lower_bound(&tree, data, exact);
		return result;
	}
	iterator
	upperBound(memtx_tree_key_data<Type> *data, bool *exact) const
	{
		iterator result;
		result.iterator = memtx_tree_normal_upper_bound(&tree, data, exact);
		return result;
	}
	iterator
	first() const
	{
		iterator result;
		result.iterator = memtx_tree_normal_iterator_first(&tree);
		return result;
	}
	iterator
	last() const
	{
		iterator result;
		result.iterator = memtx_tree_normal_iterator_last(&tree);
		return result;
	}
	iterator
	invalid() const
	{
		iterator result;
		result.iterator = memtx_tree_normal_invalid_iterator();
		return result;
	}
	void
	freezeIterator(iterator &itr)
	{
		memtx_tree_normal_iterator_freeze(&tree, &itr.iterator);
	}
	void
	destroyIterator(iterator &itr)
	{
		memtx_tree_normal_iterator_destroy(&tree, &itr.iterator);
	}
	bool
	next(iterator &itr) const
	{
		return memtx_tree_normal_iterator_next(&tree, &itr.iterator);
	}
	bool
	prev(iterator &itr) const
	{
		return memtx_tree_normal_iterator_prev(&tree, &itr.iterator);
	}
	size_t
	size() const
	{
		return memtx_tree_normal_size(&tree);
	}
	size_t
	memUsed() const
	{
		return memtx_tree_normal_mem_used(&tree);
	}
	int
	build(memtx_tree_data<Type> *data, size_t size)
	{
		return memtx_tree_normal_build(&tree, data, size);

	}
	int
	insert(struct tuple *tuple)
	{
		struct memtx_tree_data<Type> data(tuple, tree.arg);
		return memtx_tree_normal_insert(&tree, data, NULL);
	}
	int
	insert(struct tuple *tuple, struct tuple **replaced)
	{
		struct memtx_tree_data<Type> data(tuple, tree.arg);
		struct memtx_tree_data<Type> repl_data;
		int res =  memtx_tree_normal_insert(&tree, data, &repl_data);
		*replaced = repl_data.tuple;
		return res;
	}
	void
	remove(struct tuple *tuple)
	{
		struct memtx_tree_data<Type> data(tuple, tree.arg);
		memtx_tree_normal_delete(&tree, data);
	}

private:
	memtx_tree_normal tree;
};

template <>
class treeProxy<MEMTX_TREE_TYPE_HINTED> {
public:
	/** Constant for general/partial specialization compatibility */
	enum { Type = MEMTX_TREE_TYPE_HINTED };
	struct iterator	{
		memtx_tree_hinted_iterator iterator;
	};
	void
	create(struct key_def *def,
	       bps_tree_extent_alloc_f alloc, bps_tree_extent_free_f free)
	{
		memtx_tree_hinted_create(&tree, def, alloc, free, NULL);
	}
	void
	destroy()
	{
		memtx_tree_hinted_destroy(&tree);
	}
	memtx_tree_data<Type> *
	get(struct iterator &it) const
	{
		return memtx_tree_hinted_iterator_get_elem(&tree, &it.iterator);
	}
	memtx_tree_data<Type> *
	random(uint32_t seed) const
	{
		return memtx_tree_hinted_random(&tree, seed);
	}
	memtx_tree_data<Type> *
	find(memtx_tree_key_data<Type> *key_data) const
	{
		return memtx_tree_hinted_find(&tree, key_data);
	}
	iterator
	lowerBound(const memtx_tree_data<Type> &data, bool *exact) const
	{
		iterator result;
		result.iterator = memtx_tree_hinted_lower_bound_elem(&tree, data, exact);
		return result;
	}
	iterator
	upperBound(const memtx_tree_data<Type> &data, bool *exact) const
	{
		iterator result;
		result.iterator = memtx_tree_hinted_upper_bound_elem(&tree, data, exact);
		return result;
	}
	iterator
	lowerBound(memtx_tree_key_data<Type> *data, bool *exact) const
	{
		iterator result;
		result.iterator = memtx_tree_hinted_lower_bound(&tree, data, exact);
		return result;
	}
	iterator
	upperBound(memtx_tree_key_data<Type> *data, bool *exact) const
	{
		iterator result;
		result.iterator = memtx_tree_hinted_upper_bound(&tree, data, exact);
		return result;
	}
	iterator
	first() const
	{
		iterator result;
		result.iterator = memtx_tree_hinted_iterator_first(&tree);
		return result;
	}
	iterator
	last() const
	{
		iterator result;
		result.iterator = memtx_tree_hinted_iterator_last(&tree);
		return result;
	}
	iterator
	invalid() const
	{
		iterator result;
		result.iterator = memtx_tree_hinted_invalid_iterator();
		return result;
	}
	void
	freezeIterator(iterator &itr)
	{
		memtx_tree_hinted_iterator_freeze(&tree, &itr.iterator);
	}
	void
	destroyIterator(iterator &itr)
	{
		memtx_tree_hinted_iterator_destroy(&tree, &itr.iterator);
	}
	bool
	next(iterator &itr) const
	{
		return memtx_tree_hinted_iterator_next(&tree, &itr.iterator);
	}
	bool
	prev(iterator &itr) const
	{
		return memtx_tree_hinted_iterator_prev(&tree, &itr.iterator);
	}
	size_t
	size() const
	{
		return memtx_tree_hinted_size(&tree);
	}
	size_t
	memUsed() const
	{
		return memtx_tree_hinted_mem_used(&tree);
	}
	int
	build(memtx_tree_data<Type> *data, size_t size)
	{
		return memtx_tree_hinted_build(&tree, data, size);

	}
	int
	insert(struct tuple *tuple)
	{
		struct memtx_tree_data<Type> data(tuple, tree.arg);
		return memtx_tree_hinted_insert(&tree, data, NULL);
	}
	int
	insert(struct tuple *tuple, struct tuple **replaced)
	{
		struct memtx_tree_data<Type> data(tuple, tree.arg);
		struct memtx_tree_data<Type> repl_data;
		int res =  memtx_tree_hinted_insert(&tree, data, &repl_data);
		*replaced = repl_data.tuple;
		return res;
	}
	void
	remove(struct tuple *tuple)
	{
		struct memtx_tree_data<Type> data(tuple, tree.arg);
		memtx_tree_hinted_delete(&tree, data);
	}

private:
	memtx_tree_hinted tree;
};

template <>
class treeProxy<MEMTX_TREE_TYPE_HINT_ONLY> {
public:
	/** Constant for general/partial specialization compatibility */
	enum { Type = MEMTX_TREE_TYPE_HINT_ONLY };
	struct iterator	{
		memtx_tree_hint_only_iterator iterator;
	};
	void
	create(struct key_def *def,
	       bps_tree_extent_alloc_f alloc, bps_tree_extent_free_f free)
	{
		memtx_tree_hint_only_create(&tree, def, alloc, free, NULL);
	}
	void
	destroy()
	{
		memtx_tree_hint_only_destroy(&tree);
	}
	memtx_tree_data<Type> *
	get(struct iterator &it) const
	{
		return memtx_tree_hint_only_iterator_get_elem(&tree, &it.iterator);
	}
	memtx_tree_data<Type> *
	random(uint32_t seed) const
	{
		return memtx_tree_hint_only_random(&tree, seed);
	}
	memtx_tree_data<Type> *
	find(memtx_tree_key_data<Type> *key_data) const
	{
		return memtx_tree_hint_only_find(&tree, key_data);
	}
	iterator
	lowerBound(const memtx_tree_data<Type> &data, bool *exact) const
	{
		iterator result;
		result.iterator = memtx_tree_hint_only_lower_bound_elem(&tree, data, exact);
		return result;
	}
	iterator
	upperBound(const memtx_tree_data<Type> &data, bool *exact) const
	{
		iterator result;
		result.iterator = memtx_tree_hint_only_upper_bound_elem(&tree, data, exact);
		return result;
	}
	iterator
	lowerBound(memtx_tree_key_data<Type> *data, bool *exact) const
	{
		iterator result;
		result.iterator = memtx_tree_hint_only_lower_bound(&tree, data, exact);
		return result;
	}
	iterator
	upperBound(memtx_tree_key_data<Type> *data, bool *exact) const
	{
		iterator result;
		result.iterator = memtx_tree_hint_only_upper_bound(&tree, data, exact);
		return result;
	}
	iterator
	first() const
	{
		iterator result;
		result.iterator = memtx_tree_hint_only_iterator_first(&tree);
		return result;
	}
	iterator
	last() const
	{
		iterator result;
		result.iterator = memtx_tree_hint_only_iterator_last(&tree);
		return result;
	}
	iterator
	invalid() const
	{
		iterator result;
		result.iterator = memtx_tree_hint_only_invalid_iterator();
		return result;
	}
	void
	freezeIterator(iterator &itr)
	{
		memtx_tree_hint_only_iterator_freeze(&tree, &itr.iterator);
	}
	void
	destroyIterator(iterator &itr)
	{
		memtx_tree_hint_only_iterator_destroy(&tree, &itr.iterator);
	}
	bool
	next(iterator &itr) const
	{
		return memtx_tree_hint_only_iterator_next(&tree, &itr.iterator);
	}
	bool
	prev(iterator &itr) const
	{
		return memtx_tree_hint_only_iterator_prev(&tree, &itr.iterator);
	}
	size_t
	size() const
	{
		return memtx_tree_hint_only_size(&tree);
	}
	size_t
	memUsed() const
	{
		return memtx_tree_hint_only_mem_used(&tree);
	}
	int
	build(memtx_tree_data<Type> *data, size_t size)
	{
		return memtx_tree_hint_only_build(&tree, data, size);

	}
	int
	insert(struct tuple *tuple)
	{
		struct memtx_tree_data<Type> data(tuple, tree.arg);
		return memtx_tree_hint_only_insert(&tree, data, NULL);
	}
	int
	insert(struct tuple *tuple, struct tuple **replaced)
	{
		struct memtx_tree_data<Type> data(tuple, tree.arg);
		struct memtx_tree_data<Type> repl_data;
		int res =  memtx_tree_hint_only_insert(&tree, data, &repl_data);
		*replaced = repl_data.tuple;
		return res;
	}
	void
	remove(struct tuple *tuple)
	{
		struct memtx_tree_data<Type> data(tuple, tree.arg);
		memtx_tree_hint_only_delete(&tree, data);
	}

private:
	memtx_tree_hint_only tree;
};

#endif /* TARANTOOL_BOX_MEMTX_TREE_PROXY_H_INCLUDED */
