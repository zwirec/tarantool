#include "tuple_extract_key.h"
#include "tuple.h"
#include "fiber.h"

enum { MSGPACK_NULL = 0xc0 };

/** True if key part i and i+1 are sequential. */
template <bool has_json_paths>
static inline bool
key_def_parts_are_sequential(const struct key_def *def, int i)
{
	const struct key_part *part1 = &def->parts[i];
	const struct key_part *part2 = &def->parts[i + 1];
	if (!has_json_paths) {
		return part1->fieldno + 1 == part2->fieldno;
	} else {
		return part1->fieldno + 1 == part2->fieldno &&
		       part1->path == NULL && part2->path == NULL;
	}
}

/** True, if a key con contain two or more parts in sequence. */
static bool
key_def_contains_sequential_parts(const struct key_def *def)
{
	for (uint32_t i = 0; i < def->part_count - 1; ++i) {
		if (key_def_parts_are_sequential<true>(def, i))
			return true;
	}
	return false;
}

/**
 * Optimized version of tuple_extract_key_raw() for sequential key defs
 * @copydoc tuple_extract_key_raw()
 */
template <bool has_optional_parts>
static char *
tuple_extract_key_sequential_raw(const char *data, const char *data_end,
				 struct key_def *key_def, uint32_t *key_size)
{
	assert(!has_optional_parts || key_def->is_nullable);
	assert(key_def_is_sequential(key_def));
	assert(has_optional_parts == key_def->has_optional_parts);
	assert(data_end != NULL);
	assert(mp_sizeof_nil() == 1);
	const char *field_start = data;
	uint32_t bsize = mp_sizeof_array(key_def->part_count);
	uint32_t field_count = mp_decode_array(&field_start);
	const char *field_end = field_start;
	uint32_t null_count;
	if (!has_optional_parts || field_count > key_def->part_count) {
		for (uint32_t i = 0; i < key_def->part_count; i++)
			mp_next(&field_end);
		null_count = 0;
	} else {
		assert(key_def->is_nullable);
		null_count = key_def->part_count - field_count;
		field_end = data_end;
		bsize += null_count * mp_sizeof_nil();
	}
	assert(field_end - field_start <= data_end - data);
	bsize += field_end - field_start;

	char *key = (char *) region_alloc(&fiber()->gc, bsize);
	if (key == NULL) {
		diag_set(OutOfMemory, bsize, "region",
			"tuple_extract_key_raw_sequential");
		return NULL;
	}
	char *key_buf = mp_encode_array(key, key_def->part_count);
	memcpy(key_buf, field_start, field_end - field_start);
	if (has_optional_parts && null_count > 0) {
		key_buf += field_end - field_start;
		memset(key_buf, MSGPACK_NULL, null_count);
	}

	if (key_size != NULL)
		*key_size = bsize;
	return key;
}

/**
 * Optimized version of tuple_extract_key() for sequential key defs
 * @copydoc tuple_extract_key()
 */
template <bool has_optional_parts>
static inline char *
tuple_extract_key_sequential(const struct tuple *tuple, struct key_def *key_def,
			     uint32_t *key_size)
{
	assert(key_def_is_sequential(key_def));
	assert(!has_optional_parts || key_def->is_nullable);
	assert(has_optional_parts == key_def->has_optional_parts);
	const char *data = tuple_data(tuple);
	const char *data_end = data + tuple->bsize;
	return tuple_extract_key_sequential_raw<has_optional_parts>(data,
								    data_end,
								    key_def,
								    key_size);
}

/**
 * General-purpose implementation of tuple_extract_key()
 * @copydoc tuple_extract_key()
 */
template <bool contains_sequential_parts, bool has_optional_parts,
	  bool has_json_paths>
static char *
tuple_extract_key_slowpath(const struct tuple *tuple,
			   struct key_def *key_def, uint32_t *key_size)
{
	assert(has_json_paths == key_def->has_json_paths);
	assert(!has_optional_parts || key_def->is_nullable);
	assert(has_optional_parts == key_def->has_optional_parts);
	assert(contains_sequential_parts ==
	       key_def_contains_sequential_parts(key_def));
	assert(mp_sizeof_nil() == 1);
	const char *data = tuple_data(tuple);
	uint32_t part_count = key_def->part_count;
	uint32_t bsize = mp_sizeof_array(part_count);
	struct tuple_format *format = tuple_format(tuple);
	const uint32_t *field_map = tuple_field_map(tuple);
	const char *tuple_end = data + tuple->bsize;

	/* Calculate the key size. */
	for (uint32_t i = 0; i < part_count; ++i) {
		const char *field;
		if (!has_json_paths) {
			field = tuple_field_raw(format, data, field_map,
						key_def->parts[i].fieldno);
		} else {
			field = tuple_field_by_part_raw(format, data, field_map,
							&key_def->parts[i]);
		}
		if (has_optional_parts && field == NULL) {
			bsize += mp_sizeof_nil();
			continue;
		}
		assert(field != NULL);
		const char *end = field;
		if (contains_sequential_parts) {
			/*
			 * Skip sequential part in order to
			 * minimize tuple_field_raw() calls.
			 */
			for (; i < part_count - 1; i++) {
				if (!key_def_parts_are_sequential
				        <has_json_paths>(key_def, i)) {
					/*
					 * End of sequential part.
					 */
					break;
				}
				if (!has_optional_parts || end < tuple_end)
					mp_next(&end);
				else
					bsize += mp_sizeof_nil();
			}
		}
		if (!has_optional_parts || end < tuple_end)
			mp_next(&end);
		else
			bsize += mp_sizeof_nil();
		bsize += end - field;
	}

	char *key = (char *) region_alloc(&fiber()->gc, bsize);
	if (key == NULL) {
		diag_set(OutOfMemory, bsize, "region", "tuple_extract_key");
		return NULL;
	}
	char *key_buf = mp_encode_array(key, part_count);
	for (uint32_t i = 0; i < part_count; ++i) {
		const char *field;
		if (!has_json_paths) {
			field = tuple_field_raw(format, data, field_map,
						key_def->parts[i].fieldno);
		} else {
			field = tuple_field_by_part_raw(format, data, field_map,
							&key_def->parts[i]);
		}
		if (has_optional_parts && field == NULL) {
			key_buf = mp_encode_nil(key_buf);
			continue;
		}
		const char *end = field;
		uint32_t null_count = 0;
		if (contains_sequential_parts) {
			/*
			 * Skip sequential part in order to
			 * minimize tuple_field_raw() calls.
			 */
			for (; i < part_count - 1; i++) {
				if (!key_def_parts_are_sequential
				        <has_json_paths>(key_def, i)) {
					/*
					 * End of sequential part.
					 */
					break;
				}
				if (!has_optional_parts || end < tuple_end)
					mp_next(&end);
				else
					++null_count;
			}
		}
		if (!has_optional_parts || end < tuple_end)
			mp_next(&end);
		else
			++null_count;
		bsize = end - field;
		memcpy(key_buf, field, bsize);
		key_buf += bsize;
		if (has_optional_parts && null_count != 0) {
			memset(key_buf, MSGPACK_NULL, null_count);
			key_buf += null_count * mp_sizeof_nil();
		}
	}
	if (key_size != NULL)
		*key_size = key_buf - key;
	return key;
}

/**
 * General-purpose version of tuple_extract_key_raw()
 * @copydoc tuple_extract_key_raw()
 */
template <bool has_optional_parts, bool has_json_paths>
static char *
tuple_extract_key_slowpath_raw(const char *data, const char *data_end,
			       struct key_def *key_def, uint32_t *key_size)
{
	assert(has_json_paths == key_def->has_json_paths);
	assert(!has_optional_parts || key_def->is_nullable);
	assert(has_optional_parts == key_def->has_optional_parts);
	assert(mp_sizeof_nil() == 1);
	/* allocate buffer with maximal possible size */
	char *key = (char *) region_alloc(&fiber()->gc, data_end - data);
	if (key == NULL) {
		diag_set(OutOfMemory, data_end - data, "region",
			 "tuple_extract_key_raw");
		return NULL;
	}
	char *key_buf = mp_encode_array(key, key_def->part_count);
	const char *field0 = data;
	uint32_t field_count = mp_decode_array(&field0);
	/*
	 * A tuple can not be empty - at least a pk always exists.
	 */
	assert(field_count > 0);
	(void) field_count;
	const char *field0_end = field0;
	mp_next(&field0_end);
	const char *field = field0;
	const char *field_end = field0_end;
	uint32_t current_fieldno = 0;
	for (uint32_t i = 0; i < key_def->part_count; i++) {
		uint32_t fieldno = key_def->parts[i].fieldno;
		uint32_t null_count = 0;
		for (; i < key_def->part_count - 1; i++) {
			if (!key_def_parts_are_sequential
			        <has_json_paths>(key_def, i))
				break;
		}
		const struct key_part *part = &key_def->parts[i];
		uint32_t end_fieldno = part->fieldno;

		if (fieldno < current_fieldno) {
			/* Rewind. */
			field = field0;
			field_end = field0_end;
			current_fieldno = 0;
		}

		/*
		 * First fieldno in a key columns can be out of
		 * tuple size for nullable indexes because of
		 * absense of indexed fields. Treat such fields
		 * as NULLs.
		 */
		if (has_optional_parts && fieldno >= field_count) {
			/* Nullify entire columns range. */
			null_count = fieldno - end_fieldno + 1;
			memset(key_buf, MSGPACK_NULL, null_count);
			key_buf += null_count * mp_sizeof_nil();
			continue;
		}
		while (current_fieldno < fieldno) {
			/* search first field of key in tuple raw data */
			field = field_end;
			mp_next(&field_end);
			current_fieldno++;
		}

		/*
		 * If the last fieldno is out of tuple size, then
		 * fill rest of columns with NULLs.
		 */
		if (has_optional_parts && end_fieldno >= field_count) {
			null_count = end_fieldno - field_count + 1;
			field_end = data_end;
		} else {
			while (current_fieldno < end_fieldno) {
				mp_next(&field_end);
				current_fieldno++;
			}
		}
		const char *src = field;
		const char *src_end = field_end;
		if (part->path != NULL) {
			if (tuple_field_go_to_path(&src, part->path,
						   part->path_len) != 0) {
				/*
				 * All tuples must be valid as all
				 * integrity checks have already
				 * passed.
				 */
				unreachable();
			}
			src_end = src;
			mp_next(&src_end);
		}
		memcpy(key_buf, src, src_end - src);
		key_buf += src_end - src;
		if (has_optional_parts && null_count != 0) {
			memset(key_buf, MSGPACK_NULL, null_count);
			key_buf += null_count * mp_sizeof_nil();
		} else {
			assert(key_buf - key <= data_end - data);
		}
	}
	if (key_size != NULL)
		*key_size = (uint32_t)(key_buf - key);
	return key;
}

static const tuple_extract_key_t extract_key_slowpath_funcs[] = {
	tuple_extract_key_slowpath<false, false, false>,
	tuple_extract_key_slowpath<true, false, false>,
	tuple_extract_key_slowpath<false, true, false>,
	tuple_extract_key_slowpath<true, true, false>,
	tuple_extract_key_slowpath<false, false, true>,
	tuple_extract_key_slowpath<true, false, true>,
	tuple_extract_key_slowpath<false, true, true>,
	tuple_extract_key_slowpath<true, true, true>
};

/**
 * Initialize tuple_extract_key() and tuple_extract_key_raw()
 */
void
tuple_extract_key_set(struct key_def *key_def)
{
	if (key_def_is_sequential(key_def)) {
		if (key_def->has_optional_parts) {
			assert(key_def->is_nullable);
			key_def->tuple_extract_key =
				tuple_extract_key_sequential<true>;
			key_def->tuple_extract_key_raw =
				tuple_extract_key_sequential_raw<true>;
		} else {
			key_def->tuple_extract_key =
				tuple_extract_key_sequential<false>;
			key_def->tuple_extract_key_raw =
				tuple_extract_key_sequential_raw<false>;
		}
	} else {
		int func_idx =
			(key_def_contains_sequential_parts(key_def) ? 1 : 0) +
			2 * (key_def->has_optional_parts ? 1 : 0) +
			4 * (key_def->has_json_paths ? 1 : 0);
		key_def->tuple_extract_key =
			extract_key_slowpath_funcs[func_idx];
		assert(!key_def->has_optional_parts || key_def->is_nullable);
	}
	if (key_def->has_optional_parts) {
		assert(key_def->is_nullable);
		if (key_def->has_json_paths) {
			key_def->tuple_extract_key_raw =
				tuple_extract_key_slowpath_raw<true, true>;
		} else {
			key_def->tuple_extract_key_raw =
				tuple_extract_key_slowpath_raw<true, false>;
		}
	} else {
		if (key_def->has_json_paths) {
			key_def->tuple_extract_key_raw =
				tuple_extract_key_slowpath_raw<false, true>;
		} else {
			key_def->tuple_extract_key_raw =
				tuple_extract_key_slowpath_raw<false, false>;
		}
	}
}
