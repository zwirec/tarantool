/*
 * Copyright 2010-2018, Tarantool AUTHORS, please see AUTHORS file.
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
#include "update_field.h"
#include "trivia/util.h"
#include "json/path.h"
#include "box/tuple_format.h"

static inline int
update_bar_locate(struct update_op *op, struct update_field *field)
{
	field->bar.point = field->data;
	int rc = tuple_field_go_to_path(&field->bar.point, op->path,
					op->path_offset, op->path_len);
	if (rc > 0)
		return -1;
	if (rc != 0) {
		diag_set(ClientError, ER_NO_SUCH_FIELD_NAME,
			 tt_cstr(op->path, op->path_len));
		return -1;
	}
	const char *data = field->bar.point;
	mp_next(&data);
	field->bar.point_size = data - field->bar.point;
	field->type = UPDATE_BAR;
	field->bar.op = op;
	return 0;
}

int
do_op_bar_insert(struct update_op *op, struct update_field *field,
		 struct update_ctx *ctx)
{
	assert(op->opcode == '!');
	(void) ctx;
	if (field->type != UPDATE_NOP) {
		diag_set(ClientError, ER_UNSUPPORTED, "update",
			 "intersected JSON paths");
		return -1;
	}
	int rc;
	const char *point = field->data, *parent = NULL;

	struct json_path_parser parser;
	struct json_path_node node, next_node;
	json_path_parser_create(&parser, op->path + op->path_offset,
				op->path_len - op->path_offset);
	do {
		rc = json_path_next(&parser, &node);
		if (rc != 0)
			goto err_json;
		if (node.type == JSON_PATH_END) {
			assert(parent != NULL);
			enum mp_type type = mp_typeof(*parent);
			if (type == MP_MAP) {
				diag_set(ClientError, ER_UPDATE_FIELD_NAME,
					 tt_cstr(op->path, op->path_len),
					 "the key exists already");
				return -1;
			} else {
				assert(type == MP_ARRAY);
				op->new_field_len = op->arg.set.length;
				field->bar.point = point;
				goto success;
			}
		}
		parent = point;
		if (node.type == JSON_PATH_NUM)
			rc = tuple_field_go_to_index(&point, node.num);
		else
			rc = tuple_field_go_to_key(&point, node.str, node.len);
	} while (rc == 0);
	assert(rc == -1);
	rc = json_path_next(&parser, &next_node);
	if (rc != 0)
		goto err_json;
	if (next_node.type != JSON_PATH_END)
		goto err_no_path;

	if (node.type == JSON_PATH_NUM) {
		if (mp_typeof(*parent) != MP_ARRAY) {
			diag_set(ClientError, ER_UPDATE_FIELD_NAME,
				 tt_cstr(op->path, op->path_len),
				 "can not insert by index in non-array field");
			return -1;
		}
		op->new_field_len = op->arg.set.length;;
		field->bar.point = parent;
		uint32_t size = mp_decode_array(&field->bar.point);
		if (node.num > size)
			goto err_no_path;
		assert(node.num == size);
	} else {
		assert(node.type == JSON_PATH_STR);
		if (mp_typeof(*parent) != MP_MAP) {
			diag_set(ClientError, ER_UPDATE_FIELD_NAME,
				 tt_cstr(op->path, op->path_len),
				 "can not insert by key in non-map field");
			return -1;
		}
		op->new_field_len = mp_sizeof_str(node.len) +
				    op->arg.set.length;
		field->bar.key = node.str;
		field->bar.key_len = node.len;
	}
success:
	field->type = UPDATE_BAR;
	field->bar.op = op;
	field->bar.parent = parent;
	return 0;
err_json:
	diag_set(ClientError, ER_INVALID_JSON, rc,
		 tt_cstr(op->path, op->path_len));
	return -1;
err_no_path:
	diag_set(ClientError, ER_NO_SUCH_FIELD_NAME,
		 tt_cstr(op->path, op->path_len));
	return -1;
}

int
do_op_bar_set(struct update_op *op, struct update_field *field,
	      struct update_ctx *ctx)
{
	assert(op->opcode == '=');
	(void) ctx;
	if (field->type != UPDATE_NOP) {
		diag_set(ClientError, ER_UNSUPPORTED, "update",
			 "intersected JSON paths");
		return -1;
	}
	if (update_bar_locate(op, field) != 0)
		return -1;
	op->new_field_len = op->arg.set.length;
	return 0;
}

int
do_op_bar_delete(struct update_op *op, struct update_field *field,
		 struct update_ctx *ctx)
{
	assert(op->opcode == '#');
	(void) ctx;
	if (field->type != UPDATE_NOP) {
		diag_set(ClientError, ER_UNSUPPORTED, "update",
			 "intersected JSON paths");
		return -1;
	}
	int rc;
	const char *point = field->data, *parent = NULL;
	enum mp_type type;

	struct json_path_parser parser;
	struct json_path_node node;
	json_path_parser_create(&parser, op->path + op->path_offset,
				op->path_len - op->path_offset);
	do {
		rc = json_path_next(&parser, &node);
		if (rc != 0) {
			diag_set(ClientError, ER_INVALID_JSON, rc,
				 tt_cstr(op->path, op->path_len));
			return -1;
		}
		switch (node.type) {
		case JSON_PATH_END:
			assert(parent != NULL);
			type = mp_typeof(*parent);
			if (type == MP_MAP) {
				if (op->arg.del.count != 1) {
					diag_set(ClientError,
						 ER_UPDATE_FIELD_NAME,
						 tt_cstr(op->path,
							 op->path_len),
						 "can delete only 1 field "\
						 "from map");
					return -1;
				}
				const char *tmp = point;
				/* Skip key/value. */
				mp_next(&tmp);
				mp_next(&tmp);
				field->bar.point_size = tmp - point;
			} else {
				assert(type == MP_ARRAY);
				const char *tmp = parent;
				uint32_t size = mp_decode_array(&tmp);
				if (size < op->arg.del.count)
					op->arg.del.count = size;
				tmp = point;
				for (uint32_t i = 0; i < op->arg.del.count; ++i)
					mp_next(&tmp);
				field->bar.point_size = tmp - point;
			}
			field->type = UPDATE_BAR;
			field->bar.point = point;
			field->bar.op = op;
			field->bar.parent = parent;
			return 0;
		case JSON_PATH_NUM:
			parent = point;
			rc = tuple_field_go_to_index(&point, node.num);
			break;
		case JSON_PATH_STR:
			parent = point;
			rc = tuple_field_go_to_key(&point, node.str, node.len);
			break;
		default:
			unreachable();
		}
	} while (rc == 0);
	assert(rc == -1);
	diag_set(ClientError, ER_NO_SUCH_FIELD_NAME,
		 tt_cstr(op->path, op->path_len));
	return -1;
}

#define DO_SCALAR_OP_GENERIC(op_type) \
int \
do_op_bar_##op_type(struct update_op *op, struct update_field *field, \
		    struct update_ctx *ctx) \
{ \
	(void) ctx; \
	if (field->type != UPDATE_NOP) { \
		diag_set(ClientError, ER_UNSUPPORTED, "update", \
			 "intersected JSON paths"); \
		return -1; \
	} \
	if (update_bar_locate(op, field) != 0) \
		return -1; \
	return update_op_do_##op_type(op, field->bar.point, ctx->index_base); \
}

DO_SCALAR_OP_GENERIC(arith)

DO_SCALAR_OP_GENERIC(bit)

DO_SCALAR_OP_GENERIC(splice)

uint32_t
update_bar_sizeof(struct update_field *field)
{
	assert(field->type == UPDATE_BAR);
	switch(field->bar.op->opcode) {
	case '!': {
		assert(field->bar.parent != NULL);
		const char *tmp = field->bar.parent;
		enum mp_type type = mp_typeof(*tmp);
		uint32_t size;
		/* New header. */
		if (type == MP_MAP) {
			size = mp_decode_map(&tmp);
			size = mp_sizeof_map(size + 1);
		} else {
			assert(type == MP_ARRAY);
			size = mp_decode_array(&tmp);
			size = mp_sizeof_array(size + 1);
		}
		/* Before object. */
		size += field->bar.parent - field->data;
		/* Old objects and tail. */
		size += field->data + field->size - tmp;
		/* New [key] = value. */
		return size + field->bar.op->new_field_len;
	}
	case '#': {
		assert(field->bar.parent != NULL);
		const char *tmp = field->bar.parent;
		enum mp_type type = mp_typeof(*tmp);
		uint32_t delete_count = field->bar.op->arg.del.count;
		uint32_t size = field->size;
		/* New header. */
		if (type == MP_MAP) {
			assert(delete_count == 1);
			uint32_t old_count = mp_decode_map(&tmp);
			assert(old_count > 0);
			size = size - mp_sizeof_map(old_count) +
			       mp_sizeof_map(old_count - 1);
		} else {
			assert(type == MP_ARRAY);
			uint32_t old_count = mp_decode_array(&tmp);
			assert(old_count >= delete_count);
			size = size - mp_sizeof_array(old_count) +
			       mp_sizeof_array(old_count - delete_count);
		}
		return size - field->bar.point_size;
	}
	default: {
		uint32_t before_point = field->bar.point - field->data;
		const char *end = field->data + field->size;
		uint32_t after_point = end - (field->bar.point +
					      field->bar.point_size);
		return field->bar.op->new_field_len + before_point +
		       after_point;
	}
	}
}

uint32_t
update_bar_store(struct update_field *field, char *out, char *out_end)
{
	(void) out_end;
	struct update_op *op = field->bar.op;
	assert(field->type == UPDATE_BAR);
	switch(op->opcode) {
	case '!': {
		const char *tmp = field->bar.parent;
		enum mp_type type = mp_typeof(*tmp);
		uint32_t before_parent = tmp - field->data;
		char *out_saved = out;
		/* Before parent. */
		memcpy(out, field->data, before_parent);
		out += before_parent;
		if (type == MP_MAP) {
			/* New map header. */
			uint32_t size = mp_decode_map(&tmp);
			out = mp_encode_map(out, size + 1);
			/* New key. */
			out = mp_encode_str(out, field->bar.key,
					    field->bar.key_len);
			/* New value. */
			memcpy(out, op->arg.set.value, op->arg.set.length);
			out += op->arg.set.length;
		} else {
			assert(type == MP_ARRAY);
			/* New array header. */
			uint32_t size = mp_decode_array(&tmp);
			out = mp_encode_array(out, size + 1);
			/* Before insertion point. */
			size = field->bar.point - tmp;
			memcpy(out, tmp, size);
			out += size;
			tmp += size;
			/* New value. */
			memcpy(out, op->arg.set.value, op->arg.set.length);
			out += op->arg.set.length;
		}
		/* Old objects and tail. */
		uint32_t after_point = field->data + field->size - tmp;
		memcpy(out, tmp, after_point);
		out += after_point;
		return out - out_saved;
	}
	case '#': {
		const char *pos = field->bar.parent;
		enum mp_type type = mp_typeof(*pos);
		uint32_t size, before_parent = pos - field->data;
		char *out_saved = out;
		/* Before parent. */
		memcpy(out, field->data, before_parent);
		out += before_parent;
		if (type == MP_MAP) {
			/* New map header. */
			size = mp_decode_map(&pos);
			assert(size > 0);
			out = mp_encode_map(out, size - 1);
		} else {
			assert(type == MP_ARRAY);
			/* New array header. */
			size = mp_decode_array(&pos);
			assert(size >= op->arg.del.count);
			out = mp_encode_array(out, size - op->arg.del.count);
		}
		size = field->bar.point - pos;
		memcpy(out, pos, size);
		out += size;
		pos = field->bar.point + field->bar.point_size;

		size = field->data + field->size - pos;
		memcpy(out, pos, size);
		return out + size - out_saved;
	}
	default: {
		uint32_t before_point = field->bar.point - field->data;
		const char *end = field->data + field->size;
		uint32_t after_point = end - (field->bar.point +
					      field->bar.point_size);

		memcpy(out, field->data, before_point);
		out += before_point;
		op->meta->store_f(op, field->bar.point, out);
		out += op->new_field_len;
		memcpy(out, field->bar.point + field->bar.point_size, after_point);
		return op->new_field_len + before_point + after_point;
	}
	}
}
