#!/usr/bin/env tarantool --ignore-space-formats
--
-- gh-3605: allow to ignore space formats via command line
-- arguments.
--
local tap = require('tap')
local test = tap.test("ignore_formats")
test:plan(1)

box.cfg{}

local format = {}
format[1] = {'field1', 'unsigned'}
format[2] = {'field2', 'unsigned'}
local s = box.schema.create_space('test', {format = format})
local pk = s:create_index('pk')
test:ok(pcall(s.replace, s, {1, 'string'}), 'can violate format')
s:drop()
os.exit(test:check() == true and 0 or 1)
