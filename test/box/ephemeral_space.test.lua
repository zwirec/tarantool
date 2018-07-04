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
