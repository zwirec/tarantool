-- Ephemeral space: create and drop

s = box.schema.space.create_ephemeral()
s.index
s.engine
s.field_count
s:drop()

format = {{name='field1', type='unsigned'}, {name='field2', type='string'}}
options = {engine = 'memtx', field_count = 7, format = format}
s = box.schema.space.create_ephemeral(options)
s.index
s.engine
s.field_count
s:drop()

s = box.schema.space.create_ephemeral({engine = 'other'})
s = box.schema.space.create_ephemeral({field_count = 'asd'})
s = box.schema.space.create_ephemeral({format = 'a'})

-- Multiple creation and drop
for j = 1,10 do for i=1,10 do s = box.schema.space.create_ephemeral(); s:drop(); end; collectgarbage('collect'); end

-- Multiple drop
s = box.schema.space.create_ephemeral()
s:drop()
s:drop()

-- Drop using function from box.schema
s = box.schema.space.create_ephemeral()
box.schema.space.drop_ephemeral(s)
s


-- Ephemeral space: methods
format = {{name='field1', type='unsigned'}, {name='field2', type='string'}}
options = {engine = 'memtx', field_count = 7, format = format}
s = box.schema.space.create_ephemeral(options)
s:format()
s:run_triggers(true)
s:drop()

format = {}
format[1] = {name = 'aaa', type = 'unsigned'}
format[2] = {name = 'bbb', type = 'unsigned'}
format[3] = {name = 'ccc', type = 'unsigned'}
format[4] = {name = 'ddd', type = 'unsigned'}
s = box.schema.space.create_ephemeral({format = format})
s:frommap({ddd = 1, aaa = 2, ccc = 3, bbb = 4})
s:frommap({ddd = 1, aaa = 2, bbb = 3})
s:frommap({ddd = 1, aaa = 2, ccc = 3, eee = 4})
s:frommap()
s:frommap({})
s:frommap({ddd = 1, aaa = 2, ccc = 3, bbb = 4}, {table = true})
s:frommap({ddd = 1, aaa = 2, ccc = 3, bbb = 4}, {table = false})
s:frommap({ddd = 1, aaa = 2, ccc = 3, bbb = box.NULL})
s:frommap({ddd = 1, aaa = 2, ccc = 3, bbb = 4}, {dummy = true})
s:drop()


-- Ephemeral space: index create and drop.
s = box.schema.space.create_ephemeral()

i = s:create_index('a')
i.unique
i.parts
i.id
i.name
i:drop()

i = s:create_index('a', {parts={{5,'string', collation='Unicode'}}})
i.parts
i:drop()

i = s:create_index('a', {parts={2, 'unsigned', 3, 'unsigned'}})
i.parts
i:drop()

-- Double creation of index for ephemeral space.
i = s:create_index('a')
i = s:create_index('a')
i:drop()

i = s:create_index('a')
i = s:create_index('a', {if_not_exists=true})
i:drop()

-- Ephemeral space can have only primary index with id == 0.
i = s:create_index('a', {id = 10})

i = s:create_index('a', {type = 'bitset', parts = {1, 'unsigned', 2, 'unsigned'}})

-- Ephemeral space: methods

s = box.schema.space.create_ephemeral({field_count = 3})
i = s:create_index('a')

s:insert{1}
s:insert{2,2,2}

s:drop()

s = box.schema.space.create_ephemeral()
i = s:create_index('a', { type = 'tree', parts = {1, 'string'} })
s:insert{'1'}
s:get{'1'}
s:insert{'1'}
s:insert{1}
i:drop()

i = s:create_index('a', { type = 'tree', parts = {1, 'unsigned'} })
s:insert{1}
s:get{1}
s:insert{1}
s:insert{'1'}
i:drop()

i = s:create_index('a', { type = 'tree', parts = {1, 'string'} })
s:replace{'1'}
s:get{'1'}
s:replace{'1'}
s:replace{1}
i:drop()

i = s:create_index('a', { type = 'tree', parts = {1, 'unsigned'} })
s:replace{1}
s:get{1}
s:replace{1}
s:replace{'1'}
i:drop()

i = s:create_index('a', { type = 'tree', parts = {1, 'string'} })
s:replace{'1'}
s:get{'1'}
s:replace{'1'}
s:replace{1}
i:drop()

i = s:create_index('a', { type = 'tree', parts = {1, 'unsigned'} })
s:upsert({1, 0}, {{'+', 2, 1}})
s:get{1}
s:upsert({1, 0}, {{'+', 2, 1}})
s:get{1}
s:upsert({1, 0}, {{'+', 1, 1}})
s:get{1}
s:get{2}

s:upsert({'1'}, {{'!', 2, 100}})
s:upsert({1}, {{'a', 2, 100}})

i:drop()

i = s:create_index('a')
s:insert{1, 2, 3, 4, 5}
s:update({1}, {{'#', 1, 1}})
s:update({1}, {{'#', 1, "only one record please"}})
i:drop()

i = s:create_index('a')
s:insert{1, 0}
s:update(1, {{'+', 2, 10}})
s:update(1, {{'+', 2, 15}})
s:update(1, {{'-', 2, 5}})
s:update(1, {{'-', 2, 20}})
s:update(1, {{'|', 2, 0x9}})
s:update(1, {{'|', 2, 0x6}})
s:update(1, {{'&', 2, 0xabcde}})
s:update(1, {{'&', 2, 0x2}})
s:update(1, {{'^', 2, 0xa2}})
s:update(1, {{'^', 2, 0xa2}})
i:drop()

i = s:create_index('a')
s:insert{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
s:update({0}, {{'#', 42, 1}})
s:update({0}, {{'#', 4, 'abirvalg'}})
s:update({0}, {{'#', 2, 1}, {'#', 4, 2}, {'#', 6, 1}})
s:update({0}, {{'#', 4, 3}})
s:update({0}, {{'#', 5, 123456}})
s:update({0}, {{'#', 3, 4294967295}})
s:update({0}, {{'#', 2, 0}})
i:drop()



i = s:create_index('a', { type = 'tree', parts = {1, 'string'} })
s:insert{'1'}
s:insert{'5'}
s:insert{'6'}
s:insert{'11'}
t = {} for state, v in i:pairs({}, {iterator = 'ALL'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs({}, {iterator = 'GE'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs('5', {iterator = 'GE'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs('5', {iterator = 'GT'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs({}, {iterator = 'LE'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs('5', {iterator = 'LE'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs({}, {iterator = 'LT'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs('5', {iterator = 'LT'}) do table.insert(t, v) end
t
i:drop()

i = s:create_index('a', { type = 'tree', parts = {1, 'unsigned'} })
s:insert{1}
s:insert{5}
s:insert{11}
t = {} for state, v in i:pairs({}, {iterator = 'ALL'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs({}, {iterator = 'GE'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs(5, {iterator = 'GE'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs(5, {iterator = 'GT'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs({}, {iterator = 'LE'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs(5, {iterator = 'LE'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs({}, {iterator = 'LT'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs(5, {iterator = 'LT'}) do table.insert(t, v) end
t
i:drop()

i = s:create_index('a', { type = 'tree', parts = {1, 'unsigned', 2, 'unsigned'} })
s:insert{1, 1}
s:insert{5, 5}
s:insert{11, 11}
t = {} for state, v in i:pairs({}, {iterator = 'ALL'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs({}, {iterator = 'GE'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs({5, 5}, {iterator = 'GE'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs({5, 5}, {iterator = 'GT'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs({}, {iterator = 'LE'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs({5, 5}, {iterator = 'LE'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs({}, {iterator = 'LT'}) do table.insert(t, v) end
t
t = {} for state, v in i:pairs({5, 5}, {iterator = 'LT'}) do table.insert(t, v) end
t
i:drop()

i = s:create_index('a')
s:auto_increment{1}
s:auto_increment{2}
s:auto_increment{3}
s:pairs(2, 'GE'):totable()
i:count({2}, 'GT')
i:drop()

i = s:create_index('a', { type = 'tree', parts = {1, 'unsigned'} })
s:replace{1}
s:replace{2}
s:delete{1}
s:delete{2}
s:select()
i:drop()
s:drop()

test_run = require('test_run').new()
utils = dofile('utils.lua')

s = box.schema.space.create_ephemeral()
idx = s:create_index('a')
for i = 1, 13 do s:insert{ i, string.rep('x', i) } end
s:len()
s:bsize()
utils.space_bsize(s)

for i = 1, 13, 2 do s:delete{ i } end
s:len()
s:bsize()
utils.space_bsize(s)

for i = 2, 13, 2 do s:update( { i }, {{ ":", 2, i, 0, string.rep('y', i) }} ) end
s:len()
s:bsize()
utils.space_bsize(s)
idx:drop()

i = s:create_index('a', { type = 'tree', parts = {1, 'string'} })
s:insert({'1', "AAAA"})
s:insert({'2', "AAAA"})
s:insert({'3', "AAAA"})
s:insert({'4', "AAAA"})

i:select()
i:max('2')
i:min('2')
i:count('2')
i:max()
i:min()
i:count()

s:insert({'20', "AAAA"})
s:insert({'30', "AAAA"})
s:insert({'40', "AAAA"})

s:select()
i:max('15')
i:min('15')
s:count('15')
i:max()
i:min()
s:count()

s:insert({'-2', "AAAA"})
s:insert({'-3', "AAAA"})
s:insert({'-4', "AAAA"})

i:select()
i:max('0')
i:min('0')
i:count('0')
i:max()
i:min()
i:count()

s:drop()

test_run:cmd("restart server default")
