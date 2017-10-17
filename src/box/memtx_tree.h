#ifndef TARANTOOL_BOX_MEMTX_TREE_H_INCLUDED
#define TARANTOOL_BOX_MEMTX_TREE_H_INCLUDED
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

#include "memtx_index.h"
#include "memtx_engine.h"
#include "tuple_compare.h"

/**
 * Struct that is used as a key in BPS tree definition.
 */
template <bool isHint>
struct memtx_tree_key_data {
	/** Sequence of msgpacked search fields */
	const char *key;
	/** Number of msgpacked search fields */
	uint32_t part_count;
	/** Set up structure after settinh key and part count */
	void prepare(const struct key_def *def) { (void)def; }
};

template<>
struct memtx_tree_key_data<true> {
	/** Compare hint */
	uint64_t hint;
	/** Sequence of msgpacked search fields */
	const char *key;
	/** Number of msgpacked search fields */
	uint32_t part_count;
	/** Set up structure after settinh key and part count */
	void prepare(const struct key_def *def) { hint = key_hint(key, def); }
};


/**
 * Struct that is used as an unit of storage in BPS tree.
 */
template <bool isHint>
struct memtx_tree_data {
	struct tuple *tuple;
	bool operator == (const memtx_tree_data &a) const
	{
		return tuple == a.tuple;
	}
	bool operator != (const memtx_tree_data &a) const
	{
		return tuple != a.tuple;
	}
	int compare(const memtx_tree_data &a, struct key_def *def) const
	{
		return tuple_compare(tuple, a.tuple, def);
	}
	int compare(const memtx_tree_key_data<isHint> &a, struct key_def *def) const
	{
		return tuple_compare_with_key(tuple, a.key, a.part_count, def);
	}
};

template <>
struct memtx_tree_data<true> {
	uint64_t hint;
	struct tuple *tuple;
	bool operator == (const memtx_tree_data &a) const
	{
		return tuple == a.tuple;
	}
	bool operator != (const memtx_tree_data &a) const
	{
		return tuple != a.tuple;
	}
	int compare(const memtx_tree_data &a, struct key_def *def) const
	{
		if (hint != a.hint) {
			return hint < a.hint ? -1 : 1;
		}
		return tuple_compare(tuple, a.tuple, def);
	}
	int compare(const memtx_tree_key_data<true> &a, struct key_def *def) const
	{
		if (a.part_count == 0)
			return 0;
		if (hint != a.hint) {
			return hint < a.hint ? -1 : 1;
		}
		return tuple_compare_with_key(tuple, a.key, a.part_count, def);
	}
};

#define BPS_TREE_NAME memtx_tree_normal
#define BPS_TREE_BLOCK_SIZE (512)
#define BPS_TREE_EXTENT_SIZE MEMTX_EXTENT_SIZE
#define BPS_TREE_COMPARE(a, b, arg) a->compare(b, arg)
#define BPS_TREE_COMPARE_KEY(a, b, arg) a->compare(b, arg)
#define bps_tree_elem_t memtx_tree_data<false>
#define bps_tree_key_t memtx_tree_key_data<false>
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
#define BPS_TREE_COMPARE(a, b, arg) a->compare(b, arg)
#define BPS_TREE_COMPARE_KEY(a, b, arg) a->compare(b, arg)
#define bps_tree_elem_t memtx_tree_data<true>
#define bps_tree_key_t memtx_tree_key_data<true>
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

template <bool isHinted>
struct treeProxy {
	memtx_tree_normal tree;
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
	memtx_tree_data<isHinted> *
	get(struct iterator *it) const
	{
		return memtx_tree_normal_iterator_get_elem(&tree, &it->iterator);
	}
	memtx_tree_data<isHinted> *
	random(uint32_t seed) const
	{
		return memtx_tree_normal_random(&tree, seed);
	}
	memtx_tree_data<isHinted> *
	find(const memtx_tree_key_data<isHinted> &key_data) const
	{
		return memtx_tree_normal_find(&tree, key_data);
	}
	iterator
	lowerBound(const memtx_tree_data<isHinted> &data, bool *exact)
	{
		iterator result;
		result.iterator = memtx_tree_normal_lower_bound_elem(&tree, data, exact);
		return result;
	}
	iterator
	upperBound(const memtx_tree_data<isHinted> &data, bool *exact)
	{
		iterator result;
		result.iterator = memtx_tree_normal_upper_bound_elem(&tree, data, exact);
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
	bool
	next(const iterator &itr)
	{
		return memtx_tree_normal_iterator_next(&tree, &itr.iterator);
	}
	bool
	prev(const iterator &itr)
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
};

template <>
struct treeProxy<true> {
	memtx_tree_hinted tree;
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
	memtx_tree_data<true> *
	get(struct iterator *it) const
	{
		return memtx_tree_hinted_iterator_get_elem(&tree, &it->iterator);
	}
	memtx_tree_data<true> *
	random(uint32_t seed)  const
	{
		return memtx_tree_hinted_random(&tree, seed);
	}
	memtx_tree_data<true> *
	find(const memtx_tree_key_data<true> &key_data) const
	{
		return memtx_tree_hinted_find(&tree, key_data);
	}
	iterator
	lowerBound(const memtx_tree_data<true> &data, bool *exact)
	{
		iterator result;
		result.iterator = memtx_tree_hinted_lower_bound_elem(&tree, data, exact);
		return result;
	}
	iterator
	upperBound(const memtx_tree_data<true> &data, bool *exact)
	{
		iterator result;
		result.iterator = memtx_tree_hinted_upper_bound_elem(&tree, data, exact);
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
	bool
	next(const iterator &itr)
	{
		return memtx_tree_hinted_iterator_next(&tree, &itr.iterator);
	}
	bool
	prev(const iterator &itr)
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
};


template<bool isHinted>
class MemtxTree: public MemtxIndex {
public:
	MemtxTree(struct index_def *index_def);
	virtual ~MemtxTree() override;

	virtual void beginBuild() override;
	virtual void reserve(uint32_t size_hint) override;
	virtual void buildNext(struct tuple *tuple) override;
	virtual void endBuild() override;
	virtual size_t size() const override;
	virtual struct tuple *random(uint32_t rnd) const override;
	virtual struct tuple *findByKey(const char *key,
					uint32_t part_count) const override;
	virtual struct tuple *replace(struct tuple *old_tuple,
				      struct tuple *new_tuple,
				      enum dup_replace_mode mode) override;

	virtual size_t bsize() const override;
	virtual struct iterator *allocIterator() const override;
	virtual void initIterator(struct iterator *iterator,
				  enum iterator_type type,
				  const char *key,
				  uint32_t part_count) const override;

	/**
	 * Create an ALL iterator with personal read view so further
	 * index modifications will not affect the iteration results.
	 * Must be destroyed by iterator->free after usage.
	 */
	struct snapshot_iterator *createSnapshotIterator() override;

private:
	/**
	 * key def that is used in tree comparison.
	 * See MemtxTree(struct index_def *) for details.
	 */
	struct key_def *cmp_def;
	treeProxy<isHinted> tree;
	memtx_tree_data<isHinted> *build_array;
	size_t build_array_size, build_array_alloc_size;
};

#endif /* TARANTOOL_BOX_MEMTX_TREE_H_INCLUDED */
