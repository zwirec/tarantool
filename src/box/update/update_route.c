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
#include "diag.h"
#include "small/region.h"
#include "json/path.h"
#include "update_field.h"
#include "box/error.h"
#include "box/tuple_format.h"

int
update_route_branch_array(struct update_field *field, struct update_op *new_op,
			  const char *parent,
			  const struct json_path_parser *old_parser,
			  const struct json_path_node *old_node,
			  const struct json_path_parser *new_parser,
			  const struct json_path_node *new_node,
			  int saved_new_offset, struct update_ctx *ctx)
{
	assert(type == MP_ARRAY);
	if (new_node->type != JSON_PATH_NUM) {
		return update_err(new_op, ctx->index_base, "can not "\
				  "update array by non-integer index");
	}
	if (new_node->num == 0) {
		return update_err_no_such_field(new_op,
						ctx->index_base);
	}
	/* Already validated for the previous operation. */
	assert(old_node->type == JSON_PATH_NUM);
	assert(old_node->num > 0);
	int32_t field_no = old_node->num - 1;
	struct update_field *next_hop;
	bool turn_into_array = saved_new_offset == 0;
	if (! turn_into_array) {
		next_hop = (struct update_field *)
			   region_alloc(ctx->region, sizeof(*next_hop));
		if (next_hop == NULL) {
			diag_set(OutOfMemory, sizeof(*next_hop),
				 "region_alloc", "next_hop");
			return -1;
		}
	} else {
		next_hop = field;
	}
	if (json_path_parser_is_eof(old_parser)) {
		assert(field->type == UPDATE_BAR);
		/*
		 * Transform bar update into terminal one.
		 */
		struct update_op *old_op = field->bar.op;
		old_op->field_no = field_no;
		old_op->path_offset += old_parser.offset;
		const char *data = parent;
		uint32_t field_count = mp_decode_array(&data);
		const char *end = data;
		for (uint32_t i = 0; i < field_count; ++i)
			mp_next(&end);
		if (update_array_create(next_hop, ctx->region, parent,
					data, end, field_count) != 0)
			return -1;
		if (old_op->meta->do_f(old_op, next_hop, ctx) != 0)
			return -1;
	} else {
		struct update_op tmp = *field;
		/*
		 * Propagate bar/route path with no
		 * re-applying.
		 */
		if (tmp.type == UPDATE_ROUTE) {
			tmp.route.path += old_parser.offset;
		} else {
			assert(tmp.type == UPDATE_BAR);
			tmp.bar.op->path_offset += old_parser.offset;
			tmp.bar.op->field_no = field_no;
		}
		if (update_array_create_with_child(next_hop, &tmp,
						   field_no,
						   ctx->region,
						   parent) != 0)
			return -1;
	}
	if (! turn_into_array) {
		field->type = UPDATE_ROUTE;
		field->route.path = new_op->path + new_op->path_offset;
		field->route.path_len = saved_new_offset;
		field->route.next_hop = next_hop;
	}
	new_op->field_no = new_node.num - 1;
	new_op->path_offset += new_parser->offset;
	return 0;
}

int
update_route_branch(struct update_field *field, struct update_op *new_op,
		    struct update_ctx *ctx)
{
	assert(new_op->path != NULL);
	const char *old_path;
	int old_path_len;
	if (field->type == UPDATE_BAR) {
		struct update_op *old_op = field->bar.op;
		old_path = old_op->path + old_op->path_offset;
		old_path_len = old_op->path_len - old_op->path_offset;
	} else {
		assert(field->type == UPDATE_ROUTE);
		old_path = field->route.path;
		old_path_len = field->route.path_len;
	}
	assert(old_path != NULL);
	struct json_path_parser old_parser, new_parser;
	struct json_path_node old_node, new_node;
	int saved_new_offset;
	json_path_parser_create(&old_parser, old_path, old_path_len);
	json_path_parser_create(&new_parser, new_op->path + new_op->path_offset,
				new_op->path_len - new_op->path_offset);
	const char *parent = field->data;
	const char *unused_mp_key;
	int rc;
	do {
		rc = json_path_next(&old_parser, &old_node);
		/* Old path is already validated. */
		assert(rc == 0);
		saved_new_offset = new_parser.offset;
		rc = json_path_next(&new_parser, &new_node);
		if (rc != 0) {
			diag_set(ClientError, ER_INVALID_JSON, rc +
				 new_op->path_offset, new_op->path_len,
				 new_op->path);
			return -1;
		}
		if (! json_path_node_eq(&old_node, &new_node))
			break;
		switch(new_node.type) {
		case JSON_PATH_NUM:
			rc = tuple_field_go_to_index(&parent, new_node.num);
			break;
		case JSON_PATH_STR:
			rc = tuple_field_go_to_key(&parent, new_node.str,
						   new_node.len,
						   &unused_mp_key);
			break;
		default:
			assert(new_node.type == JSON_PATH_END);
			rc = -1;
			break;
		}
		if (rc != 0)
			return update_err_double(new_op, ctx->index_base);
	} while (true);
	enum mp_type type = mp_typeof(*parent);
	if (type == MP_MAP) {
		diag_set(ClientError, ER_UNSUPPORTED, "update",
			 "path intersection on map");
		return -1;
	} else {
		return update_route_branch_array(field, new_op, parent,
						 &old_parser, &old_node,
						 &new_parser, &new_node,
						 saved_new_offset, ctx);
	}
}

static struct update_field *
update_route_next(struct update_field *field, struct update_op *op,
		  struct update_ctx *ctx)
{
	assert(field->type == UPDATE_ROUTE);
	assert(! update_op_is_term(op));
	const char *new_path = op->path + op->path_offset;
	int new_path_len = op->path_len - op->path_offset;
	if (field->route.path_len <= new_path_len &&
	    memcmp(field->route.path, new_path, field->route.path_len) == 0) {
		/*
		 * Fast path: jump to the next hop with no
		 * decoding. Is used, when several JSON updates
		 * have the same prefix.
		 */
		op->path_offset += field->route.path_len;
		struct json_path_parser parser;
		json_path_parser_create(&parser, op->path + op->path_offset,
					op->path_len - op->path_offset);
		struct json_path_node node;
		int rc = json_path_next(&parser, &node);
		if (rc != 0) {
			diag_set(ClientError, ER_INVALID_JSON, rc +
				 op->path_offset, op->path_len, op->path);
			return NULL;
		}
		op->path_offset += parser.offset;
		switch (node.type) {
		case JSON_PATH_NUM:
			if (node.num == 0) {
				update_err_no_such_field(op, ctx->index_base);
				return NULL;
			}
			op->field_no = node.num - 1;
			break;
		case JSON_PATH_STR:
			diag_set(ClientError, ER_UNSUPPORTED, "update",
				 "path intersection on map");
			return NULL;
		default:
			break;
		}
	} else if (update_route_branch(field, op, ctx) != 0) {
		return NULL;
	}
	return field->route.next_hop;
}

int
do_op_route_set(struct update_op *op, struct update_field *field,
		struct update_ctx *ctx)
{
	assert(op->opcode == '=');
	assert(field->type == UPDATE_ROUTE);
	struct update_field *next_hop = update_route_next(field, op, ctx);
	if (next_hop == NULL)
		return -1;
	return do_op_set(op, next_hop, ctx);
}

#define DO_SCALAR_OP_GENERIC(op_type) \
int \
do_op_route_##op_type(struct update_op *op, struct update_field *field, \
		      struct update_ctx *ctx) \
{ \
	(void) op; \
	(void) field; \
	(void) ctx; \
	return -1; \
}

DO_SCALAR_OP_GENERIC(insert)

DO_SCALAR_OP_GENERIC(delete)

DO_SCALAR_OP_GENERIC(arith)

DO_SCALAR_OP_GENERIC(bit)

DO_SCALAR_OP_GENERIC(splice)

uint32_t
update_route_sizeof(struct update_field *field)
{
	return field->size - field->route.next_hop->size +
	       update_field_sizeof(field->route.next_hop);
}

uint32_t
update_route_store(struct update_field *field, char *out, char *out_end)
{
	char *saved_out = out;
	int before_hop = field->route.next_hop->data - field->data;
	memcpy(out, field->data, before_hop);
	out += before_hop;
	out += update_field_store(field->route.next_hop, out, out_end);
	int after_hop = before_hop + field->route.next_hop->size;
	memcpy(out, field->data + after_hop, field->size - after_hop);
	return out + field->size - after_hop - saved_out;
}
