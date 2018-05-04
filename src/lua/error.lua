-- error.lua (internal file)

local ffi = require('ffi')
ffi.cdef[[
enum { METHOD_ARG_MAX = 8 };

struct method_info {
    const struct type_info *owner;
    const char *name;
    enum ctype rtype;
    enum ctype atype[METHOD_ARG_MAX];
    int nargs;
    bool isconst;

    union {
        /* Add extra space to get proper struct size in C */
        void *_spacer[2];
    };
};

char *
exception_get_string(struct error *e, const struct method_info *method);
int
exception_get_int(struct error *e, const struct method_info *method);

]]

local error = require("error")

local REFLECTION_CACHE = {}

local function reflection_enumerate(err)
    local key = tostring(err._type)
    local result = REFLECTION_CACHE[key]
    if result ~= nil then
        return result
    end
    result = {}
    -- See type_foreach_method() in reflection.h
    local t = err._type
    while t ~= nil do
        local m = t.methods
        while m.name ~= nil do
            result[ffi.string(m.name)] = m
            m = m + 1
        end
        t = t.parent
    end
    REFLECTION_CACHE[key] = result
    return result
end

local function reflection_get(err, method)
    if method.nargs ~= 0 then
        return nil -- NYI
    end
    if method.rtype == ffi.C.CTYPE_INT then
        return tonumber(ffi.C.exception_get_int(err, method))
    elseif method.rtype == ffi.C.CTYPE_CONST_CHAR_PTR then
        local str = ffi.C.exception_get_string(err, method)
        if str == nil then
            return nil
        end
        return ffi.string(str)
    end
end

local function error_type(err)
    return ffi.string(err._type.name)
end

local function error_trace(err)
    if err.depth_traceback == 0 then
        return {}
    end
    return error.get_traceback(err)
end

local function error_message(err)
    return ffi.string(err._errmsg)
end


local error_fields = {
    ["type"]        = error_type;
    ["message"]     = error_message;
    ["trace"]       = error_trace;
}

local function error_unpack(err)
    if not ffi.istype('struct error', err) then
        error("Usage: error:unpack()")
    end
    local result = {}
    for key, getter in pairs(error_fields)  do
        result[key] = getter(err)
    end
    for key, getter in pairs(reflection_enumerate(err)) do
        local value = reflection_get(err, getter)
        if value ~= nil then
            result[key] = value
        end
    end
    return result
end

local function error_raise(err)
    if not ffi.istype('struct error', err) then
        error("Usage: error:raise()")
    end
    error(err)
end

local function error_match(err, ...)
    if not ffi.istype('struct error', err) then
        error("Usage: error:match()")
    end
    return string.match(error_message(err), ...)
end

local function error_serialize(err)
    -- Return an error message only in admin console to keep compatibility
    return error_message(err)
end

local error_methods = {
    ["unpack"] = error_unpack;
    ["raise"] = error_raise;
    ["match"] = error_match; -- Tarantool 1.6 backward compatibility
    ["__serialize"] = error_serialize;
}

local function error_index(err, key)
    local getter = error_fields[key]
    if getter ~= nil then
        return getter(err)
    end
    getter = reflection_enumerate(err)[key]
    if getter ~= nil and getter.nargs == 0 then
        local val = reflection_get(err, getter)
        if val ~= nil then
            return val
        end
    end
    return error_methods[key]
end


local error_mt = {
    __index = error_index;
    __tostring = error_message;
};

ffi.metatype('struct error', error_mt);

return error