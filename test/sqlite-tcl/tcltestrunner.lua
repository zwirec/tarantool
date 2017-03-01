#!/usr/bin/env tarantool
local tarantool, scriptfile = assert(arg[-1]), assert(arg[1])
if not scriptfile then
    error(string.format('Usage: %s SCRIPT', arg[0]))
end

-- loading libtestfixture - dynamic librarary for running SQL tests
local ffi = require('ffi')
local builddir = string.gsub(tarantool, 'tarantool$', 'lib/sqlite/src')
local libpath = string.format('%s/?.so;%s/?.dylib', builddir, builddir)
local lib = ffi.load(package.searchpath('libtestfixture', libpath))
if not lib then
    error('libtestfixture missing')
end

ffi.cdef [[
    int testfixture_main(int argc, const char *argv[]);
]]

local function runtests()
    local args = ffi.new('const char *[?]', 3)
    args[0] = tarantool
    args[1] = scriptfile
    return lib.testfixture_main(2, args)
end

-- cleanup
os.execute("rm -f *.snap *.xlog*")

-- configuring tarantool
box.cfg {
    listen = os.getenv("LISTEN"),
    logger="sql.log",
}

os.exit(runtests())
