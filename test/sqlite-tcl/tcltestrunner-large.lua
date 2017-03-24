#!/usr/bin/env tarantool

-- Invoke tcltestrunner.lua on a large stack.
-- Some TCL tests crash with default 64K stack.
--
-- Use sparingly since, generally, SQL should work fine with default
-- stack.

local thisfile = assert(arg[0])
local fiber = require('fiber')
local ffi = require('ffi')
ffi.cdef("extern int fiber_stack_pages")
ffi.C.fiber_stack_pages = 64
fiber.create(function()
   dofile(string.gsub(thisfile, '-large[.]lua', '.lua'))
end)
