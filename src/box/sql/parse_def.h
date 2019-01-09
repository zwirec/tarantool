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
#include "sqliteInt.h"
#include "box/fkey.h"

/**
 * This file contains auxiliary structures and functions which
 * are used only during parsing routine (see parse.y).
 * Their main purpose is to assemble common parts of altered
 * entities (such as name, or IF EXISTS clause) and pass them
 * as a one object to further functions.
 *
 * Hierarchy is following:
 *
 * Base structure is ALTER.
 * ALTER is omitted only for CREATE TABLE since table is filled
 * with meta-information just-in-time of parsing:
 * for instance, as soon as field's name and type are recognized
 * they are added to space definition.
 *
 * DROP is general for all existing objects and includes
 * name of object itself, name of parent object (table),
 * IF EXISTS clause and may contain on-drop behaviour
 * (CASCADE/RESTRICT, but now it is always RESTRICT).
 * Hence, it terms of grammar - it is a terminal symbol.
 *
 * RENAME can be applied only to table (at least now, since it is
 * ANSI extension), so it is also terminal symbol.
 *
 * CREATE in turn can be expanded to nonterminal symbol
 * CREATE CONSTRAINT or to terminal CREATE TABLE/INDEX/TRIGGER.
 * CREATE CONSTRAINT unfolds to FOREIGN KEY or UNIQUE/PRIMARY KEY.
 *
 * For instance:
 * ALTER TABLE t ADD CONSTRAINT c FOREIGN KEY REFERENCES t2(id);
 * ALTER *TABLE* -> CREATE ENTITY -> CREATE CONSTRAINT -> CREATE FK
 *
 * CREATE TRIGGER tr1 ...
 * ALTER *TABLE* -> CREATE ENTITY -> CREATE TRIGGER
 */
struct alter_entity_def {
	/** As a rule it is a name of table to be altered. */
	struct SrcList *entity_name;
};

struct rename_entity_def {
	struct alter_entity_def *base;
	struct Token new_name;
};

struct create_entity_def {
	struct alter_entity_def *base;
	struct Token name;
	/** Statement comes with IF NOT EXISTS clause. */
	bool if_not_exist;
};

struct drop_entity_def {
	struct alter_entity_def *base;
	/** Name of index/trigger/constraint to be dropped. */
	struct Token name;
	/** Statement comes with IF EXISTS clause. */
	bool if_exist;
};

struct create_trigger_def {
	struct create_entity_def *base;
	/** One of TK_BEFORE, TK_AFTER, TK_INSTEAD. */
	int tr_tm;
	/** One of TK_INSERT, TK_UPDATE, TK_DELETE. */
	int op;
	/** Column list if this is an UPDATE trigger. */
	struct IdList *cols;
	/** When clause. */
	struct Expr *when;
};

struct create_constraint_def {
	struct create_entity_def *base;
	/** One of DEFERRED, IMMEDIATE. */
	bool is_deferred;
};

struct create_fk_def {
	struct create_constraint_def *base;
	struct ExprList *child_cols;
	struct Token *parent_name;
	struct ExprList *parent_cols;
	/**
	 * Encoded actions for MATCH, ON DELETE and
	 * ON UPDATE clauses.
	 */
	int actions;
};

struct create_index_def {
	struct create_constraint_def *base;
	/** List of indexed columns. */
	struct ExprList *cols;
	/** One of _PRIMARY_KEY, _UNIQUE, _NON_UNIQUE. */
	enum sql_index_type idx_type;
	enum sort_order sort_order;
};


/**
 * Below is a list of *_def constructors. All of them allocate
 * memory for new object using parser's region: it simplifies
 * things since their lifetime is restricted by parser.
 *
 * In case of OOM, they return NULL and set appropriate
 * error code in parser's structure and re-raise error
 * via diag_set().
 */
struct alter_entity_def *
alter_entity_def_new(struct Parse *parse, struct SrcList *name);

struct rename_entity_def *
rename_entity_def_new(struct Parse *parse, struct alter_entity_def *base,
		      struct Token new_name);

struct create_entity_def *
create_entity_def_new(struct Parse *parse, struct alter_entity_def *base,
		      struct Token name, bool if_not_exists);

struct drop_entity_def *
drop_entity_def_new(struct Parse *parse, struct alter_entity_def *base,
		    struct Token name, bool if_exist);

struct create_constraint_def *
create_constraint_def_new(struct Parse *parse, struct create_entity_def *base,
			  bool is_deferred);

struct create_fk_def *
create_fk_def_new(struct Parse *parse, struct create_constraint_def *base,
		  struct ExprList *child_cols, struct Token *parent_name,
		  struct ExprList *parent_cols, int actions);

struct create_index_def *
create_index_def_new(struct Parse *parse, struct create_constraint_def *base,
		     struct ExprList *cols, enum sql_index_type idx_type,
		     enum sort_order sort_order);
