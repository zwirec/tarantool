#ifndef TARANTOOL_BOX_TUPLE_FORMAT_H_INCLUDED
#define TARANTOOL_BOX_TUPLE_FORMAT_H_INCLUDED
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

#include "key_def.h"
#include "field_def.h"
#include "errinj.h"
#include "json/json.h"
#include "tuple_dictionary.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

/**
 * Destroy tuple format subsystem and free resourses
 */
void
tuple_format_free();

enum { FORMAT_ID_MAX = UINT16_MAX - 1, FORMAT_ID_NIL = UINT16_MAX };
enum { FORMAT_REF_MAX = INT32_MAX};

/*
 * We don't pass TUPLE_INDEX_BASE around dynamically all the time,
 * at least hard code it so that in most cases it's a nice error
 * message
 */
enum { TUPLE_INDEX_BASE = 1 };
/*
 * A special value to indicate that tuple format doesn't store
 * an offset for a field_id.
 */
enum { TUPLE_OFFSET_SLOT_NIL = INT32_MAX };

struct tuple;
struct tuple_format;
struct coll;

/** Engine-specific tuple format methods. */
struct tuple_format_vtab {
	/**
	 * Free allocated tuple using engine-specific
	 * memory allocator.
	 */
	void
	(*tuple_delete)(struct tuple_format *format, struct tuple *tuple);
	/**
	 * Allocates a new tuple on the same allocator
	 * and with the same format.
	 */
	struct tuple*
	(*tuple_new)(struct tuple_format *format, const char *data,
	             const char *end);
};

/** Tuple field meta information for tuple_format. */
struct tuple_field {
	/** Unique field identifier. */
	uint32_t id;
	/**
	 * Field type of an indexed field.
	 * If a field participates in at least one of space indexes
	 * then its type is stored in this member.
	 * If a field does not participate in an index
	 * then UNKNOWN is stored for it.
	 */
	enum field_type type;
	/**
	 * Offset slot in field map in tuple. Normally tuple
	 * stores field map - offsets of all fields participating
	 * in indexes. This allows quick access to most used
	 * fields without parsing entire mspack. This member
	 * stores position in the field map of tuple for current
	 * field. If the field does not participate in indexes
	 * then it has no offset in field map and INT_MAX is
	 * stored in this member. Due to specific field map in
	 * tuple (it is stored before tuple), the positions in
	 * field map is negative.
	 */
	int32_t offset_slot;
	/** True if this field is used by an index. */
	bool is_key_part;
	/** Action to perform if NULL constraint failed. */
	enum on_conflict_action nullable_action;
	/** Collation definition for string comparison */
	struct coll *coll;
	/** Collation identifier. */
	uint32_t coll_id;
	/** Link in tuple_format::fields. */
	struct json_token token;
};

/**
 * Get is_nullable property of tuple_field.
 * @param tuple_field for which attribute is being fetched
 *
 * @retval boolean nullability attribute
 */
static inline bool
tuple_field_is_nullable(struct tuple_field *tuple_field)
{
	return tuple_field->nullable_action == ON_CONFLICT_ACTION_NONE;
}

/**
 * @brief Tuple format
 * Tuple format describes how tuple is stored and information about its fields
 */
struct tuple_format {
	/** Virtual function table */
	struct tuple_format_vtab vtab;
	/** Pointer to engine-specific data. */
	void *engine;
	/** Identifier */
	uint16_t id;
	/** Reference counter */
	int refs;
	/**
	 * Tuples of this format belong to a temporary space and
	 * hence can be freed immediately while checkpointing is
	 * in progress.
	 */
	bool is_temporary;
	/**
	 * Size of field map of tuple in bytes.
	 * \sa struct tuple
	 */
	uint16_t field_map_size;
	/**
	 * If not set (== 0), any tuple in the space can have any number of
	 * fields. If set, each tuple must have exactly this number of fields.
	 */
	uint32_t exact_field_count;
	/**
	 * The longest field array prefix in which the last
	 * element is used by an index.
	 */
	uint32_t index_field_count;
	/**
	 * The minimal field count that must be specified.
	 * index_field_count <= min_field_count <= field_count.
	 */
	uint32_t min_field_count;
	/**
	 * Total number of formatted fields, including JSON
	 * path fields. See also tuple_format::fields.
	 */
	uint32_t total_field_count;
	/**
	 * Bitmap of fields that must be present in a tuple
	 * conforming to the format. Indexed by tuple_field::id.
	 */
	void *required_fields;
	/**
	 * Shared names storage used by all formats of a space.
	 */
	struct tuple_dictionary *dict;
	/**
	 * Fields comprising the format, organized in a tree.
	 * First level nodes correspond to tuple fields.
	 * Deeper levels define indexed JSON paths within
	 * tuple fields. Nodes of the tree are linked by
	 * tuple_field::token.
	 */
	struct json_tree fields;
};

/**
 * Return the number of top-level tuple fields defined by
 * a given format.
 */
static inline uint32_t
tuple_format_field_count(struct tuple_format *format)
{
	const struct json_token *root = &format->fields.root;
	return root->children != NULL ? root->max_child_idx + 1 : 0;
}

/**
 * Return meta information of a top-level tuple field given
 * a format and a field index.
 */
static inline struct tuple_field *
tuple_format_field(struct tuple_format *format, uint32_t fieldno)
{
	assert(fieldno < tuple_format_field_count(format));
	struct json_token token;
	token.type = JSON_TOKEN_NUM;
	token.num = fieldno;
	return json_tree_lookup_entry(&format->fields, &format->fields.root,
				      &token, struct tuple_field, token);
}

extern struct tuple_format **tuple_formats;

static inline uint32_t
tuple_format_id(struct tuple_format *format)
{
	assert(tuple_formats[format->id] == format);
	return format->id;
}

static inline struct tuple_format *
tuple_format_by_id(uint32_t tuple_format_id)
{
	return tuple_formats[tuple_format_id];
}

/** Delete a format with zero ref count. */
void
tuple_format_delete(struct tuple_format *format);

static inline void
tuple_format_ref(struct tuple_format *format)
{
	assert((uint64_t)format->refs + 1 <= FORMAT_REF_MAX);
	format->refs++;
}

static inline void
tuple_format_unref(struct tuple_format *format)
{
	assert(format->refs >= 1);
	if (--format->refs == 0)
		tuple_format_delete(format);
}

/**
 * Allocate, construct and register a new in-memory tuple format.
 * @param vtab Virtual function table for specific engines.
 * @param engine Pointer to storage engine.
 * @param keys Array of key_defs of a space.
 * @param key_count The number of keys in @a keys array.
 * @param space_fields Array of fields, defined in a space format.
 * @param space_field_count Length of @a space_fields.
 * @param exact_field_count Exact field count for format.
 * @param is_temporary Set if format belongs to temporary space.
 *
 * @retval not NULL Tuple format.
 * @retval     NULL Memory error.
 */
struct tuple_format *
tuple_format_new(struct tuple_format_vtab *vtab, void *engine,
		 struct key_def * const *keys, uint16_t key_count,
		 const struct field_def *space_fields,
		 uint32_t space_field_count, uint32_t exact_field_count,
		 struct tuple_dictionary *dict, bool is_temporary);

/**
 * Check, if @a format1 can store any tuples of @a format2. For
 * example, if a field is not nullable in format1 and the same
 * field is nullable in format2, or the field type is integer
 * in format1 and unsigned in format2, then format1 can not store
 * format2 tuples.
 * @param format1 tuple format to check for compatibility of
 * @param format2 tuple format to check compatibility with
 *
 * @retval True, if @a format1 can store any tuples of @a format2.
 */
bool
tuple_format1_can_store_format2_tuples(struct tuple_format *format1,
				       struct tuple_format *format2);

/**
 * Calculate minimal field count of tuples with specified keys and
 * space format.
 * @param keys Array of key definitions of indexes.
 * @param key_count Length of @a keys.
 * @param space_fields Array of fields from a space format.
 * @param space_field_count Length of @a space_fields.
 *
 * @retval Minimal field count.
 */
uint32_t
tuple_format_min_field_count(struct key_def * const *keys, uint16_t key_count,
			     const struct field_def *space_fields,
			     uint32_t space_field_count);

typedef struct tuple_format box_tuple_format_t;

/** \cond public */

/**
 * Return new in-memory tuple format based on passed key definitions.
 *
 * \param keys array of keys defined for the format
 * \key_count count of keys
 * \retval new tuple format if success
 * \retval NULL for error
 */
box_tuple_format_t *
box_tuple_format_new(struct key_def **keys, uint16_t key_count);

/**
 * Increment tuple format ref count.
 *
 * \param tuple_format the tuple format to ref
 */
void
box_tuple_format_ref(box_tuple_format_t *format);

/**
 * Decrement tuple format ref count.
 *
 * \param tuple_format the tuple format to unref
 */
void
box_tuple_format_unref(box_tuple_format_t *format);

/** \endcond public */

/**
 * Fill the field map of tuple with field offsets.
 * @param format    Tuple format.
 * @param field_map A pointer behind the last element of the field
 *                  map.
 * @param tuple     MessagePack array.
 * @param validate  If set, validate the tuple against the format.
 *
 * @retval  0 Success.
 * @retval -1 Format error.
 *            +-------------------+
 * Result:    | offN | ... | off1 |
 *            +-------------------+
 *                                ^
 *                             field_map
 * tuple + off_i = indexed_field_i;
 */
int
tuple_init_field_map(struct tuple_format *format, uint32_t *field_map,
		     const char *tuple, bool validate);

/**
 * Get a field at the specific position in this MessagePack array.
 * Returns a pointer to MessagePack data.
 * @param format tuple format
 * @param tuple a pointer to MessagePack array
 * @param field_map a pointer to the LAST element of field map
 * @param field_no the index of field to return
 *
 * @returns field data if field exists or NULL
 * @sa tuple_init_field_map()
 */
static inline const char *
tuple_field_raw(struct tuple_format *format, const char *tuple,
		const uint32_t *field_map, uint32_t field_no)
{
	if (likely(field_no < format->index_field_count)) {
		/* Indexed field */

		if (field_no == 0) {
			mp_decode_array(&tuple);
			return tuple;
		}

		int32_t offset_slot = tuple_format_field(format,
					field_no)->offset_slot;
		if (offset_slot != TUPLE_OFFSET_SLOT_NIL) {
			if (field_map[offset_slot] != 0)
				return tuple + field_map[offset_slot];
			else
				return NULL;
		}
	}
	ERROR_INJECT(ERRINJ_TUPLE_FIELD, return NULL);
	uint32_t field_count = mp_decode_array(&tuple);
	if (unlikely(field_no >= field_count))
		return NULL;
	for (uint32_t k = 0; k < field_no; k++)
		mp_next(&tuple);
	return tuple;
}

/**
 * Get tuple field by its name.
 * @param format Tuple format.
 * @param tuple MessagePack tuple's body.
 * @param field_map Tuple field map.
 * @param name Field name.
 * @param name_len Length of @a name.
 * @param name_hash Hash of @a name.
 *
 * @retval not NULL MessagePack field.
 * @retval     NULL No field with @a name.
 */
static inline const char *
tuple_field_raw_by_name(struct tuple_format *format, const char *tuple,
			const uint32_t *field_map, const char *name,
			uint32_t name_len, uint32_t name_hash)
{
	uint32_t fieldno;
	if (tuple_fieldno_by_name(format->dict, name, name_len, name_hash,
				  &fieldno) != 0)
		return NULL;
	return tuple_field_raw(format, tuple, field_map, fieldno);
}

/**
 * Get tuple field by its path.
 * @param format Tuple format.
 * @param tuple MessagePack tuple's body.
 * @param field_map Tuple field map.
 * @param path Field path.
 * @param path_len Length of @a path.
 * @param path_hash Hash of @a path.
 * @param[out] field Found field, or NULL, if not found.
 *
 * @retval  0 Success.
 * @retval -1 Error in JSON path.
 */
int
tuple_field_raw_by_path(struct tuple_format *format, const char *tuple,
                        const uint32_t *field_map, const char *path,
                        uint32_t path_len, uint32_t path_hash,
                        const char **field);

/**
 * Get a tuple field pointed to by an index part.
 * @param format Tuple format.
 * @param tuple A pointer to MessagePack array.
 * @param field_map A pointer to the LAST element of field map.
 * @param part Index part to use.
 * @retval Field data if the field exists or NULL.
 */
static inline const char *
tuple_field_by_part_raw(struct tuple_format *format, const char *data,
			const uint32_t *field_map, struct key_part *part)
{
	return tuple_field_raw(format, data, field_map, part->fieldno);
}

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* #ifndef TARANTOOL_BOX_TUPLE_FORMAT_H_INCLUDED */
