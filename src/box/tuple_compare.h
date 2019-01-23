#ifndef TARANTOOL_BOX_TUPLE_COMPARE_H_INCLUDED
#define TARANTOOL_BOX_TUPLE_COMPARE_H_INCLUDED
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
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#include "key_def.h"
#include "tuple.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

/**
 * Return the length of the longest common prefix of two tuples.
 * @param tuple_a first tuple
 * @param tuple_b second tuple
 * @param key_def key defintion
 * @return number of key parts the two tuples have in common
 */
uint32_t
tuple_common_key_parts(const struct tuple *tuple_a, const struct tuple *tuple_b,
		       struct key_def *key_def);

/**
 * Create a comparison function for the key_def
 *
 * @param key_def key_definition
 * @returns a comparision function
 */
tuple_compare_t
tuple_compare_create(const struct key_def *key_def);

/**
 * @copydoc tuple_compare_create()
 */
tuple_compare_with_key_t
tuple_compare_with_key_create(const struct key_def *key_def);

/**
 * Get a comparison hint of a tuple.
 * Hint is such a function h(tuple) in terms of particular key_def that
 * has follows rules:
 * if h(t1) < h(t2) then t1 < t2;
 * if h(t1) > h(t2) then t1 > t2;
 * if t1 == t2 then h(t1) == h(t2);
 * These rules means that instead of direct tuple vs tuple (or tuple vs key)
 * comparison one may compare theirs hints first; and only if theirs hints
 * are equal compare the tuples themselves.
 * @param tuple - tuple to get hint of.
 * @param key_def - key_def that defines which comparison is used.
 * @return the hint.
 */
static inline uint64_t
tuple_hint(const struct tuple *tuple, const struct key_def *key_def)
{
	return key_def->tuple_hint(tuple, key_def);
}

/**
 * Get a comparison hint of a key.
 * @See tuple_hint for hint term definition.
 * @param key - key to get hint of.
 * @param key_def - key_def that defines which comparison is used.
 * @return the hint.
 */
static inline uint64_t
key_hint(const char *key, const struct key_def *key_def)
{
	return key_def->key_hint(key, key_def);
}

/**
 * Initialize tuple_hint() and key_hint() functions for the key_def.
 * @param key_def key definition to set up.
 */
void
tuple_hint_set(struct key_def *key_def);

/**
 * @brief Compare two fields parts using a type definition
 * @param field_a field
 * @param field_b field
 * @param field_type field type definition
 * @retval 0  if field_a == field_b
 * @retval <0 if field_a < field_b
 * @retval >0 if field_a > field_b
 */
int
tuple_compare_field(const char *field_a, const char *field_b,
		    int8_t type, struct coll *coll);

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_BOX_TUPLE_COMPARE_H_INCLUDED */
