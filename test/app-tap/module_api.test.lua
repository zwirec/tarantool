#!/usr/bin/env tarantool

local fio = require('fio')

-- Use BUILDDIR passed from test-run or cwd when run w/o
-- test-run to find test/app-tap/module_api.{so,dylib}.
build_path = os.getenv("BUILDDIR") or '.'
package.cpath = fio.pathjoin(build_path, 'test/app-tap/?.so'   ) .. ';' ..
                fio.pathjoin(build_path, 'test/app-tap/?.dylib') .. ';' ..
                package.cpath

local function test_pushcdata(test, module)
    test:plan(6)
    local ffi = require('ffi')
    ffi.cdef('struct module_api_test { int a; };')
    local gc_counter = 0;
    local ct = ffi.typeof('struct module_api_test')
    ffi.metatype(ct, {
        __tostring = function(obj)
            return 'ok'
        end;
        __gc = function(obj)
            gc_counter = gc_counter + 1;
        end
    })

    local ctid = tonumber(ct)
    local obj, ptr = module.pushcdata(ctid)
    test:is(ffi.typeof(obj), ct, 'pushcdata typeof')
    test:is(tostring(obj), 'ok', 'pushcdata metatable')
    local ctid2, ptr2 = module.checkcdata(obj)
    test:is(ctid, ctid2, 'checkcdata type')
    test:is(ptr, ptr2, 'checkcdata value')
    test:is(gc_counter, 0, 'pushcdata gc')
    obj = nil
    collectgarbage('collect')
    test:is(gc_counter, 1, 'pushcdata gc')
end

local function test_iscallable(test, module)
    local ffi = require('ffi')

    ffi.cdef([[
        struct cdata_1 { int foo; };
        struct cdata_2 { int foo; };
    ]])

    local cdata_1 = ffi.new('struct cdata_1')
    local cdata_1_ref = ffi.new('struct cdata_1 &')
    local cdata_2 = ffi.new('struct cdata_2')
    local cdata_2_ref = ffi.new('struct cdata_2 &')

    local nop = function() end

    ffi.metatype('struct cdata_2', {
        __call = nop,
    })

    local cases = {
        {
            obj = nop,
            exp = true,
            description = 'function',
        },
        {
            obj = nil,
            exp = false,
            description = 'nil',
        },
        {
            obj = 1,
            exp = false,
            description = 'number',
        },
        {
            obj = {},
            exp = false,
            description = 'table without metatable',
        },
        {
            obj = setmetatable({}, {}),
            exp = false,
            description = 'table without __call metatable field',
        },
        {
            obj = setmetatable({}, {__call = nop}),
            exp = true,
            description = 'table with __call metatable field'
        },
        {
            obj = cdata_1,
            exp = false,
            description = 'cdata without __call metatable field',
        },
        {
            obj = cdata_1_ref,
            exp = false,
            description = 'cdata reference without __call metatable field',
        },
        {
            obj = cdata_2,
            exp = true,
            description = 'cdata with __call metatable field',
        },
        {
            obj = cdata_2_ref,
            exp = true,
            description = 'cdata reference with __call metatable field',
        },
    }

    test:plan(#cases)
    for _, case in ipairs(cases) do
        test:ok(module.iscallable(case.obj, case.exp), case.description)
    end
end

local function test_luaT_new_key_def(test, module)
    local cases = {
        -- Cases to call before box.cfg{}.
        {
            'Pass a field on an unknown type',
            parts = {{
                fieldno = 2,
                type = 'unknown',
            }},
            exp_err = 'Unknown field type: unknown',
        },
        {
            'Try to use collation_id before box.cfg{}',
            parts = {{
                fieldno = 1,
                type = 'string',
                collation_id = 2,
            }},
            exp_err = 'Cannot use collations: please call box.cfg{}',
        },
        {
            'Try to use collation before box.cfg{}',
            parts = {{
                fieldno = 1,
                type = 'string',
                collation = 'unicode_ci',
            }},
            exp_err = 'Cannot use collations: please call box.cfg{}',
        },
        function()
            -- For collations.
            box.cfg{}
        end,
        -- Cases to call after box.cfg{}.
        {
            'Try to use both collation_id and collation',
            parts = {{
                fieldno = 1,
                type = 'string',
                collation_id = 2,
                collation = 'unicode_ci',
            }},
            exp_err = 'Conflicting options: collation_id and collation',
        },
        {
            'Unknown collation_id',
            parts = {{
                fieldno = 1,
                type = 'string',
                collation_id = 42,
            }},
            exp_err = 'Unknown collation_id: 42',
        },
        {
            'Unknown collation name',
            parts = {{
                fieldno = 1,
                type = 'string',
                collation = 'unknown',
            }},
            exp_err = 'Unknown collation: "unknown"',
        },
        {
            parts = 1,
            exp_err = 'Bad params, use: luaT_new_key_def({' ..
                '{fieldno = fieldno, type = type' ..
                '[, is_nullable = is_nullable' ..
                '[, collation_id = collation_id' ..
                '[, collation = collation]]]}, ...}',
        },
    }

    test:plan(#cases - 1)
    for _, case in ipairs(cases) do
        if type(case) == 'function' then
            case()
        else
            local ok, err = pcall(module.luaT_new_key_def, case.parts)
            test:is_deeply({ok, err}, {false, case.exp_err}, case[1])
        end
    end
end

local test = require('tap').test("module_api", function(test)
    test:plan(25)
    local status, module = pcall(require, 'module_api')
    test:is(status, true, "module")
    test:ok(status, "module is loaded")
    if not status then
        test:diag("Failed to load library:")
        for _, line in ipairs(module:split("\n")) do
            test:diag("%s", line)
        end
        return
    end

    -- Should be called before box.cfg{}. Calls box.cfg{} itself.
    test:test("luaT_new_key_def", test_luaT_new_key_def, module)

    local space  = box.schema.space.create("test")
    space:create_index('primary')

    for name, fun in pairs(module) do
        if string.sub(name,1, 5) == 'test_' then
            test:ok(fun(), name .. " is ok")
        end
    end

    local status, msg = pcall(module.check_error)
    test:like(msg, 'luaT_error', 'luaT_error')

    test:test("pushcdata", test_pushcdata, module)
    test:test("iscallable", test_iscallable, module)

    space:drop()
end)

os.exit(0)
