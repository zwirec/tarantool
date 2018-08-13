#!/usr/bin/env tarantool
--
-- gh-3605: allow to ignore space formats via box.cfg option.
--
local tap = require('tap')
local test = tap.test("ignore_formats")
test:plan(3)

box.cfg{ignore_space_formats=true}

local format = {}
format[1] = {'field1', 'unsigned'}
format[2] = {'field2', 'unsigned'}

local s = box.schema.create_space('t0', {format = format})
local pk = s:create_index('pk')
test:ok(pcall(s.replace, s, {1, 'string'}), 'can violate format')
-- Is not a dynamic option.
local ok, err = pcall(box.cfg, {ignore_space_formats = false})
test:ok(not ok, "ignore_space_formats is not a dynamic option")
test:ok(err:match("Can't set option"), "error message")

s:drop()

-- Cleanup xlog
box.snapshot()

os.exit(test:check() == true and 0 or 1)
