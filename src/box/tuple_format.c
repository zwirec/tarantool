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
#include "bit/bit.h"
#include "fiber.h"
#include "json/json.h"
#include "tuple_format.h"
#include "coll_id_cache.h"

/** Global table of tuple formats */
struct tuple_format **tuple_formats;
static intptr_t recycled_format_ids = FORMAT_ID_NIL;

static uint32_t formats_size = 0, formats_capacity = 0;

static struct tuple_field *
tuple_field_new(void)
{
	struct tuple_field *field = calloc(1, sizeof(struct tuple_field));
	if (field == NULL) {
		diag_set(OutOfMemory, sizeof(struct tuple_field), "malloc",
			 "tuple field");
		return NULL;
	}
	field->id = UINT32_MAX;
	field->token.type = JSON_TOKEN_END;
	field->type = FIELD_TYPE_ANY;
	field->offset_slot = TUPLE_OFFSET_SLOT_NIL;
	field->coll_id = COLL_NONE;
	field->nullable_action = ON_CONFLICT_ACTION_NONE;
	return field;
}

static void
tuple_field_delete(struct tuple_field *field)
{
	free(field);
}

/** Return path to a tuple field. Used for error reporting. */
static const char *
tuple_field_path(const struct tuple_field *field)
{
	assert(field->token.parent != NULL);
	if (field->token.parent->parent == NULL) {
		/* Top-level field, no need to format the path. */
		return int2str(field->token.num + TUPLE_INDEX_BASE);
	}
	char *path = tt_static_buf();
	json_tree_snprint_path(path, TT_STATIC_BUF_LEN, &field->token,
			       TUPLE_INDEX_BASE);
	return path;
}

/**
 * Given a field number and a path, add the corresponding tuple
 * field to the tuple format, allocating intermediate fields if
 * necessary.
 * Return a pointer to the leaf field on success, NULL on memory
 * allocation error or type/nullability mistmatch error, diag
 * message is set.
 */
static struct tuple_field *
tuple_format_add_field(struct tuple_format *format, uint32_t fieldno,
		       const char *path, uint32_t path_len)
{
	struct tuple_field *field = NULL;
	struct tuple_field *parent = tuple_format_field(format, fieldno);
	if (path_len == 0)
		goto end;
	field = tuple_field_new();
	if (field == NULL)
		goto fail;

	int rc = 0;
	uint32_t token_count = 0;
	struct json_tree *tree = &format->fields;
	struct json_lexer lexer;
	json_lexer_create(&lexer, path, path_len, TUPLE_INDEX_BASE);
	while ((rc = json_lexer_next_token(&lexer, &field->token)) == 0 &&
	       field->token.type != JSON_TOKEN_END) {
		enum field_type expected_type =
			field->token.type == JSON_TOKEN_STR ?
			FIELD_TYPE_MAP : FIELD_TYPE_ARRAY;
		if (field_type1_contains_type2(parent->type, expected_type)) {
			parent->type = expected_type;
		} else {
			diag_set(ClientError, ER_INDEX_PART_TYPE_MISMATCH,
				 tuple_field_path(parent),
				 field_type_strs[parent->type],
				 field_type_strs[expected_type]);
			goto fail;
		}
		struct tuple_field *next =
			json_tree_lookup_entry(tree, &parent->token,
					       &field->token,
					       struct tuple_field, token);
		if (next == NULL) {
			field->id = format->total_field_count++;
			rc = json_tree_add(tree, &parent->token, &field->token);
			if (rc != 0) {
				diag_set(OutOfMemory, sizeof(struct json_token),
					 "json_tree_add", "tree");
				goto fail;
			}
			next = field;
			field = tuple_field_new();
			if (field == NULL)
				goto fail;
		}
		parent = next;
		token_count++;
	}
	/* Path has been verified by key_def_decode_parts. */
	assert(rc == 0 && field->token.type == JSON_TOKEN_END);
	assert(parent != NULL);
	/* Update tree depth information. */
	format->fields_depth = MAX(format->fields_depth, token_count + 1);
end:
	tuple_field_delete(field);
	return parent;
fail:
	parent = NULL;
	goto end;
}

/**
 * Look up field metadata by identifier.
 *
 * Used only for error reporing so we can afford full field
 * tree traversal here.
 */
static struct tuple_field *
tuple_format_field_by_id(struct tuple_format *format, uint32_t id)
{
	struct tuple_field *field;
	json_tree_foreach_entry_preorder(field, &format->fields.root,
					 struct tuple_field, token) {
		if (field->id == id)
			return field;
	}
	return NULL;
}

static int
tuple_format_use_key_part(struct tuple_format *format, uint32_t field_count,
			  const struct key_part *part, bool is_sequential,
			  int *current_slot, char **path_pool)
{
	assert(part->fieldno < tuple_format_field_count(format));
	const char *path = *path_pool;
	if (part->path != NULL) {
		/**
		 * Copy JSON path data to reserved area at the
		 * end of format allocation.
		 */
		memcpy(*path_pool, part->path, part->path_len);
		*path_pool += part->path_len;
	}
	struct tuple_field *field = tuple_format_add_field(format, part->fieldno,
							   path, part->path_len);
	if (field == NULL)
		return -1;
	/*
	 * If a field is not present in the space format, inherit
	 * nullable action of the first key part referencing it.
	 */
	if (part->fieldno >= field_count && !field->is_key_part)
		field->nullable_action = part->nullable_action;
	/*
	 * Field and part nullable actions may differ only
	 * if one of them is DEFAULT, in which case we use
	 * the non-default action *except* the case when
	 * the other one is NONE, in which case we assume
	 * DEFAULT. The latter is needed so that in case
	 * index definition and space format have different
	 * is_nullable flag, we will use the strictest option,
	 * i.e. DEFAULT.
	 */
	if (field->nullable_action == ON_CONFLICT_ACTION_DEFAULT) {
		if (part->nullable_action != ON_CONFLICT_ACTION_NONE)
			field->nullable_action = part->nullable_action;
	} else if (part->nullable_action == ON_CONFLICT_ACTION_DEFAULT) {
		if (field->nullable_action == ON_CONFLICT_ACTION_NONE)
			field->nullable_action = part->nullable_action;
	} else if (field->nullable_action != part->nullable_action) {
		diag_set(ClientError, ER_ACTION_MISMATCH,
			 tuple_field_path(field),
			 on_conflict_action_strs[field->nullable_action],
			 on_conflict_action_strs[part->nullable_action]);
		return -1;
	}

	/**
	 * Check that there are no conflicts between index part
	 * types and space fields. If a part type is compatible
	 * with field's one, then the part type is more strict
	 * and the part type must be used in tuple_format.
	 */
	if (field_type1_contains_type2(field->type,
					part->type)) {
		field->type = part->type;
	} else if (!field_type1_contains_type2(part->type,
					       field->type)) {
		int errcode;
		if (!field->is_key_part)
			errcode = ER_FORMAT_MISMATCH_INDEX_PART;
		else
			errcode = ER_INDEX_PART_TYPE_MISMATCH;
		diag_set(ClientError, errcode, tuple_field_path(field),
			 field_type_strs[field->type],
			 field_type_strs[part->type]);
		return -1;
	}
	field->is_key_part = true;
	/*
	 * In the tuple, store only offsets necessary to access
	 * fields of non-sequential keys. First field is always
	 * simply accessible, so we don't store an offset for it.
	 */
	if (field->offset_slot == TUPLE_OFFSET_SLOT_NIL &&
	    is_sequential == false &&
	    (part->fieldno > 0 || part->path != NULL)) {
		*current_slot = *current_slot - 1;
		field->offset_slot = *current_slot;
	}
	return 0;
}

/**
 * Extract all available type info from keys and field
 * definitions.
 */
static int
tuple_format_create(struct tuple_format *format, struct key_def * const *keys,
		    uint16_t key_count, const struct field_def *fields,
		    uint32_t field_count)
{
	format->min_field_count =
		tuple_format_min_field_count(keys, key_count, fields,
					     field_count);
	if (tuple_format_field_count(format) == 0) {
		format->field_map_size = 0;
		return 0;
	}
	/* Initialize defined fields */
	for (uint32_t i = 0; i < field_count; ++i) {
		struct tuple_field *field = tuple_format_field(format, i);
		field->type = fields[i].type;
		field->nullable_action = fields[i].nullable_action;
		struct coll *coll = NULL;
		uint32_t cid = fields[i].coll_id;
		if (cid != COLL_NONE) {
			struct coll_id *coll_id = coll_by_id(cid);
			if (coll_id == NULL) {
				diag_set(ClientError,ER_WRONG_COLLATION_OPTIONS,
					 i + 1, "collation was not found by ID");
				return -1;
			}
			coll = coll_id->coll;
		}
		field->coll = coll;
		field->coll_id = cid;
	}

	int current_slot = 0;

	/*
	 * Set pointer to reserved area in the format chunk
	 * allocated with tuple_format_alloc call.
	 */
	char *path_pool = (char *)format + sizeof(struct tuple_format);
	/* extract field type info */
	for (uint16_t key_no = 0; key_no < key_count; ++key_no) {
		const struct key_def *key_def = keys[key_no];
		bool is_sequential = key_def_is_sequential(key_def);
		const struct key_part *part = key_def->parts;
		const struct key_part *parts_end = part + key_def->part_count;

		for (; part < parts_end; part++) {
			if (tuple_format_use_key_part(format, field_count, part,
						      is_sequential,
						      &current_slot,
						      &path_pool) != 0)
				return -1;
		}
	}

	assert(tuple_format_field(format, 0)->offset_slot ==
	       TUPLE_OFFSET_SLOT_NIL);
	size_t field_map_size = -current_slot * sizeof(uint32_t);
	if (field_map_size > UINT16_MAX) {
		/** tuple->data_offset is 16 bits */
		diag_set(ClientError, ER_INDEX_FIELD_COUNT_LIMIT,
			 -current_slot);
		return -1;
	}
	format->field_map_size = field_map_size;

	size_t required_fields_sz = bitmap_size(format->total_field_count);
	format->required_fields = calloc(1, required_fields_sz);
	if (format->required_fields == NULL) {
		diag_set(OutOfMemory, required_fields_sz,
			 "malloc", "required field bitmap");
		return -1;
	}
	format->min_tuple_size =
		mp_sizeof_array(tuple_format_field_count(format));
	struct tuple_field *field;
	json_tree_foreach_entry_preorder(field, &format->fields.root,
					 struct tuple_field, token) {
		/*
		 * Mark all leaf non-nullable fields as required
		 * by setting the corresponding bit in the bitmap
		 * of required fields.
		 */
		if (json_token_is_leaf(&field->token) &&
		    !tuple_field_is_nullable(field))
			bit_set(format->required_fields, field->id);

		/* Update format::min_tuple_size by field. */
		if (field->token.type == JSON_TOKEN_NUM) {
			/*
			 * Account a gap between omitted array
			 * items.
			 */
			struct json_token **neighbors =
				field->token.parent->children;
			for (int i = field->token.num - 1;
			     i > 0 && neighbors[i] == NULL; i--)
				format->min_tuple_size += mp_sizeof_nil();
		} else {
			/* Account a key string for map member. */
			assert(field->token.type == JSON_TOKEN_STR);
			format->min_tuple_size +=
				mp_sizeof_str(field->token.len);
		}
		int max_child_idx = field->token.max_child_idx;
		if (json_token_is_leaf(&field->token)) {
			format->min_tuple_size += mp_sizeof_nil();
		} else if (field->type == FIELD_TYPE_ARRAY) {
			format->min_tuple_size +=
				mp_sizeof_array(max_child_idx + 1);
		} else if (field->type == FIELD_TYPE_MAP) {
			format->min_tuple_size +=
				mp_sizeof_map(max_child_idx + 1);
		}
	}
	return 0;
}

static int
tuple_format_register(struct tuple_format *format)
{
	if (recycled_format_ids != FORMAT_ID_NIL) {

		format->id = (uint16_t) recycled_format_ids;
		recycled_format_ids = (intptr_t) tuple_formats[recycled_format_ids];
	} else {
		if (formats_size == formats_capacity) {
			uint32_t new_capacity = formats_capacity ?
						formats_capacity * 2 : 16;
			struct tuple_format **formats;
			formats = (struct tuple_format **)
				realloc(tuple_formats, new_capacity *
						       sizeof(tuple_formats[0]));
			if (formats == NULL) {
				diag_set(OutOfMemory,
					 sizeof(struct tuple_format), "malloc",
					 "tuple_formats");
				return -1;
			}

			formats_capacity = new_capacity;
			tuple_formats = formats;
		}
		if (formats_size == FORMAT_ID_MAX + 1) {
			diag_set(ClientError, ER_TUPLE_FORMAT_LIMIT,
				 (unsigned) formats_capacity);
			return -1;
		}
		format->id = formats_size++;
	}
	tuple_formats[format->id] = format;
	return 0;
}

static void
tuple_format_deregister(struct tuple_format *format)
{
	if (format->id == FORMAT_ID_NIL)
		return;
	tuple_formats[format->id] = (struct tuple_format *) recycled_format_ids;
	recycled_format_ids = format->id;
	format->id = FORMAT_ID_NIL;
}

/*
 * Dismantle the tuple field tree attached to the format and free
 * memory occupied by tuple fields.
 */
static void
tuple_format_destroy_fields(struct tuple_format *format)
{
	struct tuple_field *field, *tmp;
	json_tree_foreach_entry_safe(field, &format->fields.root,
				     struct tuple_field, token, tmp) {
		json_tree_del(&format->fields, &field->token);
		tuple_field_delete(field);
	}
	json_tree_destroy(&format->fields);
}

static struct tuple_format *
tuple_format_alloc(struct key_def * const *keys, uint16_t key_count,
		   uint32_t space_field_count, struct tuple_dictionary *dict)
{
	/* Size of area to store JSON paths data. */
	uint32_t path_pool_size = 0;
	uint32_t index_field_count = 0;
	/* find max max field no */
	for (uint16_t key_no = 0; key_no < key_count; ++key_no) {
		const struct key_def *key_def = keys[key_no];
		const struct key_part *part = key_def->parts;
		const struct key_part *pend = part + key_def->part_count;
		for (; part < pend; part++) {
			index_field_count = MAX(index_field_count,
						part->fieldno + 1);
			path_pool_size += part->path_len;
		}
	}
	uint32_t field_count = MAX(space_field_count, index_field_count);

	uint32_t allocation_size = sizeof(struct tuple_format) + path_pool_size;
	struct tuple_format *format = malloc(allocation_size);
	if (format == NULL) {
		diag_set(OutOfMemory, allocation_size, "malloc",
			 "tuple format");
		return NULL;
	}
	if (json_tree_create(&format->fields) != 0) {
		diag_set(OutOfMemory, 0, "json_lexer_create",
			 "tuple field tree");
		free(format);
		return NULL;
	}
	for (uint32_t fieldno = 0; fieldno < field_count; fieldno++) {
		struct tuple_field *field = tuple_field_new();
		if (field == NULL)
			goto error;
		field->id = fieldno;
		field->token.num = fieldno;
		field->token.type = JSON_TOKEN_NUM;
		if (json_tree_add(&format->fields, &format->fields.root,
				  &field->token) != 0) {
			diag_set(OutOfMemory, 0, "json_tree_add",
				 "tuple field tree entry");
			tuple_field_delete(field);
			goto error;
		}
	}
	if (dict == NULL) {
		assert(space_field_count == 0);
		format->dict = tuple_dictionary_new(NULL, 0);
		if (format->dict == NULL)
			goto error;
	} else {
		format->dict = dict;
		tuple_dictionary_ref(dict);
	}
	format->total_field_count = field_count;
	format->required_fields = NULL;
	format->fields_depth = 1;
	format->min_tuple_size = 0;
	format->refs = 0;
	format->id = FORMAT_ID_NIL;
	format->index_field_count = index_field_count;
	format->exact_field_count = 0;
	format->min_field_count = 0;
	return format;
error:
	tuple_format_destroy_fields(format);
	free(format);
	return NULL;
}

/** Free tuple format resources, doesn't unregister. */
static inline void
tuple_format_destroy(struct tuple_format *format)
{
	free(format->required_fields);
	tuple_format_destroy_fields(format);
	tuple_dictionary_unref(format->dict);
}

void
tuple_format_delete(struct tuple_format *format)
{
	tuple_format_deregister(format);
	tuple_format_destroy(format);
	free(format);
}

struct tuple_format *
tuple_format_new(struct tuple_format_vtab *vtab, struct key_def * const *keys,
		 uint16_t key_count, const struct field_def *space_fields,
		 uint32_t space_field_count, struct tuple_dictionary *dict)
{
	struct tuple_format *format =
		tuple_format_alloc(keys, key_count, space_field_count, dict);
	if (format == NULL)
		return NULL;
	format->vtab = *vtab;
	format->engine = NULL;
	format->is_temporary = false;
	if (tuple_format_register(format) < 0) {
		tuple_format_destroy(format);
		free(format);
		return NULL;
	}
	if (tuple_format_create(format, keys, key_count, space_fields,
				space_field_count) < 0) {
		tuple_format_delete(format);
		return NULL;
	}
	return format;
}

bool
tuple_format1_can_store_format2_tuples(struct tuple_format *format1,
				       struct tuple_format *format2)
{
	if (format1->exact_field_count != format2->exact_field_count)
		return false;
	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	bool ret = false;
	struct tuple_field *field1;
	json_tree_foreach_entry_preorder(field1, &format1->fields.root,
					 struct tuple_field, token) {
		region_truncate(region, region_svp);
		uint32_t path_len = json_tree_snprint_path(NULL, 0,
					&field1->token, TUPLE_INDEX_BASE);
		char *path = region_alloc(region, path_len + 1);
		if (path == NULL)
			panic("Can not allocate memory for path");
		json_tree_snprint_path(path, path_len + 1, &field1->token,
				       TUPLE_INDEX_BASE);
		struct tuple_field *field2 =
			json_tree_lookup_path_entry(&format2->fields,
				&format2->fields.root, path, path_len,
				TUPLE_INDEX_BASE, struct tuple_field, token);
		/*
		 * The field has a data type in format1, but has
		 * no data type in format2.
		 */
		if (field2 == NULL) {
			/*
			 * The field can get a name added
			 * for it, and this doesn't require a data
			 * check.
			 * If the field is defined as not
			 * nullable, however, we need a data
			 * check, since old data may contain
			 * NULLs or miss the subject field.
			 */
			if (field1->type == FIELD_TYPE_ANY &&
			    tuple_field_is_nullable(field1))
				continue;
			else
				goto out;
		}
		if (! field_type1_contains_type2(field1->type, field2->type))
			goto out;
		/*
		 * Do not allow transition from nullable to non-nullable:
		 * it would require a check of all data in the space.
		 */
		if (tuple_field_is_nullable(field2) &&
		    !tuple_field_is_nullable(field1))
			goto out;
	}
	ret = true;
out:
	region_truncate(region, region_svp);
	return ret;
}

/** @sa declaration for details. */
int
tuple_init_field_map(struct tuple_format *format, uint32_t *field_map,
		     const char *tuple, bool validate)
{
	if (tuple_format_field_count(format) == 0)
		return 0; /* Nothing to initialize */

	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);
	const char *pos = tuple;
	int rc = 0;

	/* Check to see if the tuple has a sufficient number of fields. */
	uint32_t field_count = mp_decode_array(&pos);
	if (validate && format->exact_field_count > 0 &&
	    format->exact_field_count != field_count) {
		diag_set(ClientError, ER_EXACT_FIELD_COUNT,
			 (unsigned) field_count,
			 (unsigned) format->exact_field_count);
		goto error;
	}
	/*
	 * Allocate a field bitmap that will be used for checking
	 * that all mandatory fields are present.
	 */
	void *required_fields = NULL;
	size_t required_fields_sz = 0;
	if (validate) {
		required_fields_sz = bitmap_size(format->total_field_count);
		required_fields = region_alloc(region, required_fields_sz);
		if (required_fields == NULL) {
			diag_set(OutOfMemory, required_fields_sz,
				 "region", "required field bitmap");
			goto error;
		}
		memcpy(required_fields, format->required_fields,
		       required_fields_sz);
	}
	/*
	 * Initialize the tuple field map and validate field types.
	 */
	if (field_count == 0) {
		/* Empty tuple, nothing to do. */
		goto skip;
	}
	uint32_t defined_field_count = MIN(field_count, validate ?
					   tuple_format_field_count(format) :
					   format->index_field_count);
	/*
	 * Nullify field map to be able to detect by 0,
	 * which key fields are absent in tuple_field().
	 */
	memset((char *)field_map - format->field_map_size, 0,
		format->field_map_size);
	uint32_t mp_frames_sz = format->fields_depth * sizeof(struct mp_frame);
	struct mp_frame *mp_frames = region_alloc(region, mp_frames_sz);
	if (mp_frames == NULL) {
		diag_set(OutOfMemory, mp_frames_sz, "region", "frames");
		goto error;
	}
	struct mp_stack mp_stack;
	mp_stack_init(&mp_stack, format->fields_depth, mp_frames);
	mp_stack_push(&mp_stack, MP_ARRAY, defined_field_count);
	struct tuple_field *field;
	struct json_token *parent = &format->fields.root;
	while (1) {
		while (mp_stack_advance(&mp_stack)) {
			mp_stack_pop(&mp_stack);
			if (mp_stack_is_empty(&mp_stack))
				goto skip;
			parent = parent->parent;
		}
		struct json_token token;
		token.type = mp_stack_top(&mp_stack)->type == MP_ARRAY ?
			     JSON_TOKEN_NUM : JSON_TOKEN_STR;
		if (token.type == JSON_TOKEN_NUM) {
			token.num = mp_stack_top(&mp_stack)->curr - 1;
		} else if (token.type == JSON_TOKEN_STR) {
			if (mp_typeof(*pos) != MP_STR) {
				/* Skip key. */
				mp_next(&pos);
				mp_next(&pos);
				continue;
			}
			token.str = mp_decode_str(&pos, (uint32_t *)&token.len);
		} else {
			unreachable();
		}
		enum mp_type type = mp_typeof(*pos);
		assert(parent != NULL);
		field = json_tree_lookup_entry(&format->fields, parent, &token,
					       struct tuple_field, token);
		if (field != NULL) {
			bool is_nullable = tuple_field_is_nullable(field);
			if (validate &&
			    !field_mp_type_is_compatible(field->type, type,
							 is_nullable) != 0) {
				diag_set(ClientError, ER_FIELD_TYPE,
					 tuple_field_path(field),
					 field_type_strs[field->type]);
				goto error;
			}
			if (field->offset_slot != TUPLE_OFFSET_SLOT_NIL)
				field_map[field->offset_slot] = pos - tuple;
			if (required_fields != NULL)
				bit_clear(required_fields, field->id);
		}
		if ((type == MP_ARRAY || type == MP_MAP) &&
		    !mp_stack_is_full(&mp_stack) && field != NULL) {
			uint32_t size = type == MP_ARRAY ?
					mp_decode_array(&pos) :
					mp_decode_map(&pos);
			mp_stack_push(&mp_stack, type, size);
			parent = &field->token;
		} else {
			mp_next(&pos);
		}
	};
skip:
	/*
	 * Check the required field bitmap for missing fields.
	 */
	if (required_fields != NULL) {
		struct bit_iterator it;
		bit_iterator_init(&it, required_fields,
				  required_fields_sz, true);
		size_t id = bit_iterator_next(&it);
		if (id < SIZE_MAX) {
			/* A field is missing, report an error. */
			field = tuple_format_field_by_id(format, id);
			assert(field != NULL);
			diag_set(ClientError, ER_FIELD_MISSING,
				 tuple_field_path(field));
			goto error;
		}
	}
out:
	region_truncate(region, region_svp);
	return rc;
error:
	rc = -1;
	goto out;
}

uint32_t
tuple_format_min_field_count(struct key_def * const *keys, uint16_t key_count,
			     const struct field_def *space_fields,
			     uint32_t space_field_count)
{
	uint32_t min_field_count = 0;
	for (uint32_t i = 0; i < space_field_count; ++i) {
		if (! space_fields[i].is_nullable)
			min_field_count = i + 1;
	}
	for (uint32_t i = 0; i < key_count; ++i) {
		const struct key_def *kd = keys[i];
		for (uint32_t j = 0; j < kd->part_count; ++j) {
			const struct key_part *kp = &kd->parts[j];
			if (!key_part_is_nullable(kp) &&
			    kp->fieldno + 1 > min_field_count)
				min_field_count = kp->fieldno + 1;
		}
	}
	return min_field_count;
}

/** Destroy tuple format subsystem and free resourses */
void
tuple_format_free()
{
	/* Clear recycled ids. */
	while (recycled_format_ids != FORMAT_ID_NIL) {
		uint16_t id = (uint16_t) recycled_format_ids;
		recycled_format_ids = (intptr_t) tuple_formats[id];
		tuple_formats[id] = NULL;
	}
	for (struct tuple_format **format = tuple_formats;
	     format < tuple_formats + formats_size; format++) {
		/* Do not unregister. Only free resources. */
		if (*format != NULL) {
			tuple_format_destroy(*format);
			free(*format);
		}
	}
	free(tuple_formats);
}

void
box_tuple_format_ref(box_tuple_format_t *format)
{
	tuple_format_ref(format);
}

void
box_tuple_format_unref(box_tuple_format_t *format)
{
	tuple_format_unref(format);
}

/**
 * Propagate @a field to MessagePack(field)[index].
 * @param[in][out] field Field to propagate.
 * @param index 0-based index to propagate to.
 *
 * @retval  0 Success, the index was found.
 * @retval -1 Not found.
 */
static inline int
tuple_field_go_to_index(const char **field, uint64_t index)
{
	enum mp_type type = mp_typeof(**field);
	if (type == MP_ARRAY) {
		uint32_t count = mp_decode_array(field);
		if (index >= count)
			return -1;
		for (; index > 0; --index)
			mp_next(field);
		return 0;
	} else if (type == MP_MAP) {
		index += TUPLE_INDEX_BASE;
		uint64_t count = mp_decode_map(field);
		for (; count > 0; --count) {
			type = mp_typeof(**field);
			if (type == MP_UINT) {
				uint64_t value = mp_decode_uint(field);
				if (value == index)
					return 0;
			} else if (type == MP_INT) {
				int64_t value = mp_decode_int(field);
				if (value >= 0 && (uint64_t)value == index)
					return 0;
			} else {
				/* Skip key. */
				mp_next(field);
			}
			/* Skip value. */
			mp_next(field);
		}
	}
	return -1;
}

/**
 * Propagate @a field to MessagePack(field)[key].
 * @param[in][out] field Field to propagate.
 * @param key Key to propagate to.
 * @param len Length of @a key.
 *
 * @retval  0 Success, the index was found.
 * @retval -1 Not found.
 */
static inline int
tuple_field_go_to_key(const char **field, const char *key, int len)
{
	enum mp_type type = mp_typeof(**field);
	if (type != MP_MAP)
		return -1;
	uint64_t count = mp_decode_map(field);
	for (; count > 0; --count) {
		type = mp_typeof(**field);
		if (type == MP_STR) {
			uint32_t value_len;
			const char *value = mp_decode_str(field, &value_len);
			if (value_len == (uint)len &&
			    memcmp(value, key, len) == 0)
				return 0;
		} else {
			/* Skip key. */
			mp_next(field);
		}
		/* Skip value. */
		mp_next(field);
	}
	return -1;
}

int
tuple_field_go_to_path(const char **data, const char *path, uint32_t path_len)
{
	int rc;
	struct json_lexer lexer;
	struct json_token token;
	json_lexer_create(&lexer, path, path_len, TUPLE_INDEX_BASE);
	while ((rc = json_lexer_next_token(&lexer, &token)) == 0) {
		switch (token.type) {
		case JSON_TOKEN_NUM:
			rc = tuple_field_go_to_index(data, token.num);
			break;
		case JSON_TOKEN_STR:
			rc = tuple_field_go_to_key(data, token.str, token.len);
			break;
		default:
			assert(token.type == JSON_TOKEN_END);
			return 0;
		}
		if (rc != 0) {
			*data = NULL;
			return 0;
		}
	}
	return rc;
}

const char *
tuple_field_raw_by_full_path(struct tuple_format *format, const char *tuple,
			     const uint32_t *field_map, const char *path,
			     uint32_t path_len, uint32_t path_hash)
{
	assert(path_len > 0);
	uint32_t fieldno;
	/*
	 * It is possible, that a field has a name as
	 * well-formatted JSON. For example 'a.b.c.d' or '[1]' can
	 * be field name. To save compatibility at first try to
	 * use the path as a field name.
	 */
	if (tuple_fieldno_by_name(format->dict, path, path_len, path_hash,
				  &fieldno) == 0)
		return tuple_field_raw(format, tuple, field_map, fieldno);
	struct json_lexer lexer;
	struct json_token token;
	json_lexer_create(&lexer, path, path_len, TUPLE_INDEX_BASE);
	if (json_lexer_next_token(&lexer, &token) != 0)
		return NULL;
	switch(token.type) {
	case JSON_TOKEN_NUM: {
		fieldno = token.num;
		break;
	}
	case JSON_TOKEN_STR: {
		/* First part of a path is a field name. */
		uint32_t name_hash;
		if (path_len == (uint32_t) token.len) {
			name_hash = path_hash;
		} else {
			/*
			 * If a string is "field....", then its
			 * precalculated juajit hash can not be
			 * used. A tuple dictionary hashes only
			 * name, not path.
			 */
			name_hash = field_name_hash(token.str, token.len);
		}
		if (tuple_fieldno_by_name(format->dict, token.str, token.len,
					  name_hash, &fieldno) != 0)
			return NULL;
		break;
	}
	default:
		assert(token.type == JSON_TOKEN_END);
		return NULL;
	}
	return tuple_field_raw_by_path(format, tuple, field_map, fieldno,
				       path + lexer.offset,
				       path_len - lexer.offset);
}
