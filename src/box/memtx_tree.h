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
#include "memtx_tree_proxy.h"

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
	treeProxy tree;
	memtx_tree_data *build_array;
	size_t build_array_size, build_array_alloc_size;
};

class Index *
new_memtx_tree(struct index_def *def);

#endif /* TARANTOOL_BOX_MEMTX_TREE_H_INCLUDED */
