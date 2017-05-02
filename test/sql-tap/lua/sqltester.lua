local json = require('json')
local yaml = require('yaml')
local tap = require('tap')

local test = tap.test("errno")

local function flatten(arr)
   local result = { }

   local function flatten(arr)
      for _, v in ipairs(arr) do
	 if type(v) == "table" then
	    flatten(v)
	 else
	    table.insert(result, v)
	 end
      end
   end

   flatten(arr)
   return result
end

local function finish_test()
    test:check()
    os.exit()
end
test.finish_test = finish_test

local function do_test(self, label, func, expect)
    local ok, result = pcall(func)
   if ok then
      -- do_compare(label, result, expect)
      self:is_deeply(result, expect, label)
   else
       self:fail(label)
      --io.stderr:write(string.format('%s: ERROR\n', label))
   end
end
test.do_test = do_test

local function execsql(self, sql)
   local result = box.sql.execute(sql)
   if type(result) ~= 'table' then return end

   r = flatten(result)
   for i, c in ipairs(r) do
       if c == nil then
	   r[i] = ""
       end
   end
   return r
end
test.execsql = execsql

local function catchsql(self, sql)
    r = {pcall(execsql, self, sql)}
    if r[1] == true then
	r[1] = 0
	r[2] = table.concat(r[2], " ") -- flatten result
    else
	r[1] = 1
    end
    return r
end
test.catchsql = catchsql

local function do_catchsql_test(self, label, sql, expect)
    if expect[1] == 1 then
	-- expect[2] = table.concat(expect[2], " ")
    end
    return do_test(self, label, function() return catchsql(self, sql) end, expect)
end
test.do_catchsql_test = do_catchsql_test

local function do_catchsql2_test(self, label, sql)
    return do_test(self, label, function() return catchsql2(self, sql) end)
end
test.do_catchsql2_test = do_catchsql2_test

local function do_execsql_test(self, label, sql, expect)
    return do_test(self, label, function() return execsql(self, sql) end, expect)
end
test.do_execsql_test = do_execsql_test

local function do_execsql2_test(self, label, sql)
    return do_test(self, label, function() return execsql2(self, sql) end)
end
test.do_execsql2_test = do_execsql2_test

local function execsql2(self, sql)
    local result = execsql(self, sql)
    if type(result) ~= 'table' then return end
    -- shift rows down, revealing column names
    for i = #result,0,-1 do
        result[i+1] = result[i]
    end
    local colnames = result[1]
    for i,colname in ipairs(colnames) do
        colnames[i] = colname:gsub('^sqlite_sq_[0-9a-fA-F]+','sqlite_subquery')
    end
    result[0] = nil
    return result
end
test.execsql2 = execsql2

local function sortsql(self, sql)
    local result = execsql(self, sql)
    table.sort(result, function(a,b) return a[2] < b[2] end)
    return result
end
test.sortsql = sortsql

local function catchsql2(self, sql)
    return {pcall(execsql2, self, sql)}
end
test.catchsql2 = catchsql2

local function db(cmd, ...)
    if cmd == 'eval' then
        return box.sql.execute(...)
    end
end
test.db = db

--function capable()
--    return true
--end

setmetatable(_G, nil)
os.execute("rm -f *.snap *.xlog*")

-- start the database
box.cfg()

return test
