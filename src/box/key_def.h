#ifndef TARANTOOL_BOX_KEY_DEF_H_INCLUDED
#define TARANTOOL_BOX_KEY_DEF_H_INCLUDED
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
#include "trivia/util.h"
#include "error.h"
#include "diag.h"
#include <msgpuck.h>
#include "field_def.h"
#include "coll_id.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

/* Sorting order of a part. */
extern const char *sort_order_strs[];

enum sort_order {
	SORT_ORDER_ASC = 0,
	SORT_ORDER_DESC,
	SORT_ORDER_UNDEF,
	sort_order_MAX
};

struct key_part_def {
	/** Tuple field index for this part. */
	uint32_t fieldno;
	/** Type of the tuple field. */
	enum field_type type;
	/** Collation ID for string comparison. */
	uint32_t coll_id;
	/** True if a key part can store NULLs. */
	bool is_nullable;
	/** Action to perform if NULL constraint failed. */
	enum on_conflict_action nullable_action;
	/** Part sort order. */
	enum sort_order sort_order;
	/**
	 * JSON path to indexed data, relative to the field number,
	 * or NULL if this key part indexes a top-level field.
	 * This sting is 0-terminated.
	 */
	const char *path;
};

extern const struct key_part_def key_part_def_default;

/** Descriptor of a single part in a multipart key. */
struct key_part {
	/** Tuple field index for this part */
	uint32_t fieldno;
	/** Type of the tuple field */
	enum field_type type;
	/** Collation ID for string comparison. */
	uint32_t coll_id;
	/** Collation definition for string comparison */
	struct coll *coll;
	/** Action to perform if NULL constraint failed. */
	enum on_conflict_action nullable_action;
	/** Part sort order. */
	enum sort_order sort_order;
	/**
	 * 
	 */
	bool is_multikey;
	/**
	 * JSON path to indexed data, relative to the field number,
	 * or NULL if this key part index a top-level field.
	 * This string is not 0-terminated. String memory is
	 * allocated at the end of key_def chunk.
	 */
	char *path;
	/** The length of JSON path. */
	uint32_t path_len;
	/**
	 * Epoch of the tuple format the offset slot cached in
	 * this part is valid for, see tuple_format::epoch.
	 */
	uint64_t format_epoch;
	/**
	 * Cached value of the offset slot corresponding to
	 * the indexed field (tuple_field::offset_slot).
	 * Valid only if key_def::epoch equals the epoch of
	 * the tuple format. This value is updated in
	 * tuple_field_by_part_raw to always store the
	 * offset corresponding to the last used tuple format.
	 */
	int32_t offset_slot_cache;
};

struct key_def;
struct tuple;

/**
 * Get is_nullable property of key_part.
 * @param key_part for which attribute is being fetched
 *
 * @retval boolean nullability attribute
 */
static inline bool
key_part_is_nullable(const struct key_part *part)
{
	return part->nullable_action == ON_CONFLICT_ACTION_NONE;
}

/** @copydoc tuple_compare_with_key() */
typedef int (*tuple_compare_with_key_t)(const struct tuple *tuple_a,
					const char *key,
					uint32_t part_count,
					struct key_def *key_def);
/** @copydoc tuple_compare() */
typedef int (*tuple_compare_t)(const struct tuple *tuple_a,
			       const struct tuple *tuple_b,
			       struct key_def *key_def);
/** @copydoc tuple_extract_key() */
typedef char *(*tuple_extract_key_t)(const struct tuple *tuple,
				     struct key_def *key_def,
				     uint32_t *key_size);
/** @copydoc tuple_extract_key_raw() */
typedef char *(*tuple_extract_key_raw_t)(const char *data,
					 const char *data_end,
					 struct key_def *key_def,
					 uint32_t *key_size);
/** @copydoc tuple_hash() */
typedef uint32_t (*tuple_hash_t)(const struct tuple *tuple,
				 struct key_def *key_def);
/** @copydoc key_hash() */
typedef uint32_t (*key_hash_t)(const char *key,
				struct key_def *key_def);

/** @copydoc tuple_hint() */
typedef uint64_t (*tuple_hint_t)(const struct tuple *tuple,
				 const struct key_def *key_def);

/** @copydoc key_hint() */
typedef uint64_t (*key_hint_t)(const char *key, const struct key_def *key_def);

/* Definition of a multipart key. */
struct key_def {
	/** @see tuple_compare() */
	tuple_compare_t tuple_compare;
	/** @see tuple_compare_with_key() */
	tuple_compare_with_key_t tuple_compare_with_key;
	/** @see tuple_extract_key() */
	tuple_extract_key_t tuple_extract_key;
	/** @see tuple_extract_key_raw() */
	tuple_extract_key_raw_t tuple_extract_key_raw;
	/** @see tuple_hash() */
	tuple_hash_t tuple_hash;
	/** @see key_hash() */
	key_hash_t key_hash;
	/** @see tuple_hash() */
	tuple_hint_t tuple_hint;
	/** @see key_hash() */
	key_hint_t key_hint;
	/**
	 * Minimal part count which always is unique. For example,
	 * if a secondary index is unique, then
	 * unique_part_count == secondary index part count. But if
	 * a secondary index is not unique, then
	 * unique_part_count == part count of a merged key_def.
	 */
	uint32_t unique_part_count;
	/** True, if at least one part can store NULL. */
	bool is_nullable;
	/** True if some key part has JSON path. */
	bool has_json_paths;
	/**
	 * True, if some key parts can be absent in a tuple. These
	 * fields assumed to be MP_NIL.
	 */
	bool has_optional_parts;
	/** Key fields mask. @sa column_mask.h for details. */
	uint64_t column_mask;
	/** The size of the 'parts' array. */
	uint32_t part_count;
	/** Description of parts of a multipart index. */
	struct key_part parts[];
};

/**
 * Duplicate key_def.
 * @param src Original key_def.
 *
 * @retval not NULL Duplicate of src.
 * @retval     NULL Memory error.
 */
struct key_def *
key_def_dup(const struct key_def *src);

/**
 * Swap content of two key definitions in memory.
 * The two key definitions must have the same size.
 */
void
key_def_swap(struct key_def *old_def, struct key_def *new_def);

/**
 * Delete @a key_def.
 * @param def Key_def to delete.
 */
void
key_def_delete(struct key_def *def);

/** \cond public */

typedef struct key_def box_key_def_t;
typedef struct tuple box_tuple_t;

/**
 * Create key definition with key fields with passed typed on passed positions.
 * May be used for tuple format creation and/or tuple comparison.
 *
 * \param fields array with key field identifiers
 * \param types array with key field types (see enum field_type)
 * \param part_count the number of key fields
 * \returns a new key definition object
 */
box_key_def_t *
box_key_def_new(uint32_t *fields, uint32_t *types, uint32_t part_count);

/**
 * Delete key definition
 *
 * \param key_def key definition to delete
 */
void
box_key_def_delete(box_key_def_t *key_def);

/**
 * Compare tuples using the key definition.
 * @param tuple_a first tuple
 * @param tuple_b second tuple
 * @param key_def key definition
 * @retval 0  if key_fields(tuple_a) == key_fields(tuple_b)
 * @retval <0 if key_fields(tuple_a) < key_fields(tuple_b)
 * @retval >0 if key_fields(tuple_a) > key_fields(tuple_b)
 */
int
box_tuple_compare(const box_tuple_t *tuple_a, const box_tuple_t *tuple_b,
		  box_key_def_t *key_def);

/**
 * @brief Compare tuple with key using the key definition.
 * @param tuple tuple
 * @param key key with MessagePack array header
 * @param key_def key definition
 *
 * @retval 0  if key_fields(tuple) == parts(key)
 * @retval <0 if key_fields(tuple) < parts(key)
 * @retval >0 if key_fields(tuple) > parts(key)
 */

int
box_tuple_compare_with_key(const box_tuple_t *tuple_a, const char *key_b,
			   box_key_def_t *key_def);

/** \endcond public */

static inline size_t
key_def_sizeof(uint32_t part_count, uint32_t path_pool_size)
{
	return sizeof(struct key_def) + sizeof(struct key_part) * part_count +
	       path_pool_size;
}

/**
 * Allocate a new key_def with the given part count
 * and initialize its parts.
 */
struct key_def *
key_def_new(const struct key_part_def *parts, uint32_t part_count);

/**
 * Dump part definitions of the given key def.
 * The region is used for allocating JSON paths, if any.
 * Return -1 on memory allocation error, 0 on success.
 */
int
key_def_dump_parts(const struct key_def *def, struct key_part_def *parts,
		   struct region *region);

/**
 * Update 'has_optional_parts' of @a key_def with correspondence
 * to @a min_field_count.
 * @param def Key definition to update.
 * @param min_field_count Minimal field count. All parts out of
 *        this value are optional.
 */
void
key_def_update_optionality(struct key_def *def, uint32_t min_field_count);

/**
 * An snprint-style function to print a key definition.
 */
int
key_def_snprint_parts(char *buf, int size, const struct key_part_def *parts,
		      uint32_t part_count);

/**
 * Return size of key parts array when encoded in MsgPack.
 * See also key_def_encode_parts().
 */
size_t
key_def_sizeof_parts(const struct key_part_def *parts, uint32_t part_count);

/**
 * Encode key parts array in MsgPack and return a pointer following
 * the end of encoded data.
 */
char *
key_def_encode_parts(char *data, const struct key_part_def *parts,
		     uint32_t part_count);

/**
 * Decode parts array from tuple field and write'em to index_def structure.
 * Throws a nice error about invalid types, but does not check ranges of
 *  resulting values field_no and field_type
 * Parts expected to be a sequence of <part_count> arrays like this:
 *  [NUM, STR, ..][NUM, STR, ..]..,
 *  OR
 *  {field=NUM, type=STR, ..}{field=NUM, type=STR, ..}..,
 *  The region is used for allocating JSON paths, if any.
 */
int
key_def_decode_parts(struct key_part_def *parts, uint32_t part_count,
		     const char **data, const struct field_def *fields,
		     uint32_t field_count, struct region *region);

/**
 * Returns the part in index_def->parts for the specified fieldno.
 * If fieldno is not in index_def->parts returns NULL.
 */
const struct key_part *
key_def_find_by_fieldno(const struct key_def *key_def, uint32_t fieldno);

/**
 * Returns the part in index_def->parts for the specified key part.
 * If to_find is not in index_def->parts returns NULL.
 */
const struct key_part *
key_def_find(const struct key_def *key_def, const struct key_part *to_find);

/**
 * Check if key definition @a first contains all parts of
 * key definition @a second.
 * @retval true if @a first is a superset of @a second
 * @retval false otherwise
 */
bool
key_def_contains(const struct key_def *first, const struct key_def *second);

/**
 * Allocate a new key_def with a set union of key parts from
 * first and second key defs. Parts of the new key_def consist
 * of the first key_def's parts and those parts of the second
 * key_def that were not among the first parts.
 * @retval not NULL Ok.
 * @retval NULL     Memory error.
 */
struct key_def *
key_def_merge(const struct key_def *first, const struct key_def *second);

/*
 * Check that parts of the key match with the key definition.
 * @param key_def Key definition.
 * @param key MessagePack'ed data for matching.
 * @param part_count Field count in the key.
 * @param allow_nullable True if nullable parts are allowed.
 *
 * @retval 0  The key is valid.
 * @retval -1 The key is invalid.
 */
int
key_validate_parts(const struct key_def *key_def, const char *key,
		   uint32_t part_count, bool allow_nullable);

/**
 * Return true if @a index_def defines a sequential key without
 * holes starting from the first field. In other words, for all
 * key parts index_def->parts[part_id].fieldno == part_id.
 * @param index_def index_def
 * @retval true index_def is sequential
 * @retval false otherwise
 */
static inline bool
key_def_is_sequential(const struct key_def *key_def)
{
	if (key_def->has_json_paths)
		return false;
	for (uint32_t part_id = 0; part_id < key_def->part_count; part_id++) {
		if (key_def->parts[part_id].fieldno != part_id)
			return false;
	}
	return true;
}

/**
 * Return true if @a key_def defines has fields that requires
 * special collation comparison.
 * @param key_def key_def
 * @retval true if the key_def has collation fields
 * @retval false otherwise
 */
static inline bool
key_def_has_collation(const struct key_def *key_def)
{
	for (uint32_t part_id = 0; part_id < key_def->part_count; part_id++) {
		if (key_def->parts[part_id].coll != NULL)
			return true;
	}
	return false;
}

/**
 * @brief Checks if \a field_type (MsgPack) is compatible \a type (KeyDef).
 * @param type KeyDef type
 * @param key Pointer to MsgPack field to be tested.
 * @param field_no - a field number (is used to store an error message)
 *
 * @retval 0  mp_type is valid.
 * @retval -1 mp_type is invalid.
 */
static inline int
key_part_validate(enum field_type key_type, const char *key,
		  uint32_t field_no, bool is_nullable)
{
	if (unlikely(!field_mp_type_is_compatible(key_type, mp_typeof(*key),
						  is_nullable))) {
		diag_set(ClientError, ER_KEY_PART_TYPE, field_no,
			 field_type_strs[key_type]);
		return -1;
	}
	return 0;
}

/**
 * Compare two key part arrays.
 *
 * One key part is considered to be greater than the other if:
 * - its fieldno is greater
 * - given the same fieldno, NUM < STRING
 *
 * A key part array is considered greater than the other if all
 * its key parts are greater, or, all common key parts are equal
 * but there are additional parts in the bigger array.
 */
int
key_part_cmp(const struct key_part *parts1, uint32_t part_count1,
	     const struct key_part *parts2, uint32_t part_count2);

/**
 * Extract key from tuple by given key definition and return
 * buffer allocated on box_txn_alloc with this key. This function
 * has O(n) complexity, where n is the number of key parts.
 * @param tuple - tuple from which need to extract key
 * @param key_def - definition of key that need to extract
 * @param key_size - here will be size of extracted key
 *
 * @retval not NULL Success
 * @retval NULL     Memory allocation error
 */
static inline char *
tuple_extract_key(const struct tuple *tuple, struct key_def *key_def,
		  uint32_t *key_size)
{
	return key_def->tuple_extract_key(tuple, key_def, key_size);
}

/**
 * Extract key from raw msgpuck by given key definition and return
 * buffer allocated on box_txn_alloc with this key.
 * This function has O(n*m) complexity, where n is the number of key parts
 * and m is the tuple size.
 * @param data - msgpuck data from which need to extract key
 * @param data_end - pointer at the end of data
 * @param key_def - definition of key that need to extract
 * @param key_size - here will be size of extracted key
 *
 * @retval not NULL Success
 * @retval NULL     Memory allocation error
 */
static inline char *
tuple_extract_key_raw(const char *data, const char *data_end,
		      struct key_def *key_def, uint32_t *key_size)
{
	return key_def->tuple_extract_key_raw(data, data_end, key_def,
					      key_size);
}

/**
 * Compare keys using the key definition.
 * @param key_a key parts with MessagePack array header
 * @param part_count_a the number of parts in the key_a
 * @param key_b key_parts with MessagePack array header
 * @param part_count_b the number of parts in the key_b
 * @param key_def key definition
 *
 * @retval 0  if key_a == key_b
 * @retval <0 if key_a < key_b
 * @retval >0 if key_a > key_b
 */
int
key_compare(const char *key_a, const char *key_b, struct key_def *key_def);

/**
 * Compare tuples using the key definition.
 * @param tuple_a first tuple
 * @param tuple_b second tuple
 * @param key_def key definition
 * @retval 0  if key_fields(tuple_a) == key_fields(tuple_b)
 * @retval <0 if key_fields(tuple_a) < key_fields(tuple_b)
 * @retval >0 if key_fields(tuple_a) > key_fields(tuple_b)
 */
static inline int
tuple_compare(const struct tuple *tuple_a, const struct tuple *tuple_b,
	      struct key_def *key_def)
{
	return key_def->tuple_compare(tuple_a, tuple_b, key_def);
}

/**
 * @brief Compare tuple with key using the key definition.
 * @param tuple tuple
 * @param key key parts without MessagePack array header
 * @param part_count the number of parts in @a key
 * @param key_def key definition
 *
 * @retval 0  if key_fields(tuple) == parts(key)
 * @retval <0 if key_fields(tuple) < parts(key)
 * @retval >0 if key_fields(tuple) > parts(key)
 */
static inline int
tuple_compare_with_key(const struct tuple *tuple, const char *key,
		       uint32_t part_count, struct key_def *key_def)
{
	return key_def->tuple_compare_with_key(tuple, key, part_count, key_def);
}

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_BOX_KEY_DEF_H_INCLUDED */
