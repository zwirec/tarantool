/*
 * Copyright 2010-2019, Tarantool AUTHORS, please see AUTHORS file.
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
#include "parse_def.h"

struct alter_entity_def *
alter_entity_def_new(struct Parse *parse, struct SrcList *name)
{
	size_t sz = sizeof(struct alter_entity_def);
	struct alter_entity_def *alter_def = region_alloc(&parse->region, sz);
	if (alter_def == NULL) {
		diag_set(OutOfMemory, sz, "region", "alter_def");
		parse->rc = SQL_TARANTOOL_ERROR;
		parse->nErr++;
		return NULL;
	}
	alter_def->entity_name = name;
	return alter_def;
}

struct rename_entity_def *
rename_entity_def_new(struct Parse *parse, struct alter_entity_def *base,
		      struct Token new_name)
{
	size_t sz = sizeof(struct rename_entity_def);
	struct rename_entity_def *rename_def = region_alloc(&parse->region, sz);
	if (rename_def == NULL) {
		diag_set(OutOfMemory, sz, "region", "rename_def");
		parse->rc = SQL_TARANTOOL_ERROR;
		parse->nErr++;
		return NULL;
	}
	rename_def->base = base;
	rename_def->new_name = new_name;
	return rename_def;
}

struct create_entity_def *
create_entity_def_new(struct Parse *parse, struct alter_entity_def *base,
		      struct Token name, bool if_not_exists)
{
	size_t sz = sizeof(struct create_entity_def);
	struct create_entity_def *create_def = region_alloc(&parse->region, sz);
	if (create_def == NULL) {
		diag_set(OutOfMemory, sz, "region", "create_def");
		parse->rc = SQL_TARANTOOL_ERROR;
		parse->nErr++;
		return NULL;
	}
	create_def->base = base;
	create_def->name = name;
	create_def->if_not_exist = if_not_exists;
	return create_def;
}

struct drop_entity_def *
drop_entity_def_new(struct Parse *parse, struct alter_entity_def *base,
		    struct Token entity_name, bool if_exist)
{
	size_t sz = sizeof(struct drop_entity_def);
	struct drop_entity_def *drop_def = region_alloc(&parse->region, sz);
	if (drop_def == NULL) {
		diag_set(OutOfMemory, sz, "region", "drop_def");
		parse->rc = SQL_TARANTOOL_ERROR;
		parse->nErr++;
		return NULL;
	}
	drop_def->base = base;
	drop_def->name = entity_name;
	drop_def->if_exist = if_exist;
	return drop_def;
}

struct create_constraint_def *
create_constraint_def_new(struct Parse *parse, struct create_entity_def *base,
			  bool is_deferred)
{
	size_t sz = sizeof(struct create_constraint_def);
	struct create_constraint_def *constr_def =
		region_alloc(&parse->region, sz);
	if (constr_def == NULL) {
		diag_set(OutOfMemory, sz, "region", "constr_def");
		parse->rc = SQL_TARANTOOL_ERROR;
		parse->nErr++;
		return NULL;
	}
	constr_def->base = base;
	constr_def->is_deferred = is_deferred;
	return constr_def;
}

struct create_fk_def *
create_fk_def_new(struct Parse *parse, struct create_constraint_def *base,
		  struct ExprList *child_cols, struct Token *parent_name,
		  struct ExprList *parent_cols, int actions)
{
	size_t sz = sizeof(struct create_fk_def);
	struct create_fk_def *fk_def = region_alloc(&parse->region, sz);
	if (fk_def == NULL) {
		diag_set(OutOfMemory, sz, "region", "fk_def");
		parse->rc = SQL_TARANTOOL_ERROR;
		parse->nErr++;
		return NULL;
	}
	fk_def->base = base;
	fk_def->child_cols = child_cols;
	fk_def->parent_name = parent_name;
	fk_def->parent_cols = parent_cols;
	fk_def->actions = actions;
	return fk_def;
}

struct create_index_def *
create_index_def_new(struct Parse *parse, struct create_constraint_def *base,
		     struct ExprList *cols, enum sql_index_type idx_type,
		     enum sort_order sort_order)
{
	size_t sz = sizeof(struct create_index_def);
	struct create_index_def *idx_def = region_alloc(&parse->region, sz);
	if (idx_def == NULL) {
		diag_set(OutOfMemory, sz, "region", "idx_def");
		parse->rc = SQL_TARANTOOL_ERROR;
		parse->nErr++;
		return NULL;
	}
	idx_def->base = base;
	idx_def->cols = cols;
	idx_def->idx_type = idx_type;
	idx_def->sort_order = sort_order;
	return idx_def;
}
