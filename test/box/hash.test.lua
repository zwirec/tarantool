--=============================================================================
-- 32-bit hash tests
--=============================================================================
-------------------------------------------------------------------------------
-- 32-bit hash insert fields tests
-------------------------------------------------------------------------------
hash = box.schema.space.create('tweedledum')
tmp = hash:create_index('primary', { type = 'hash', parts = {1, 'unsigned'}, unique = true })

bsize = tmp:bsize()

-- Insert valid fields
hash:insert{0, 'value1 v1.0', 'value2 v1.0'}
hash:insert{1, 'value1 v1.0', 'value2 v1.0'}
hash:insert{2, 'value1 v1.0', 'value2 v1.0'}
hash:insert{3, 'value1 v1.0', 'value2 v1.0'}

tmp:bsize() > bsize

-- Insert invalid fields
hash:insert{'invalid key', 'value1 v1.0', 'value2 v1.0'}

-------------------------------------------------------------------------------
-- 32-bit hash replace fields tests
-------------------------------------------------------------------------------

-- Replace valid fields
hash:replace{3, 'value1 v1.31', 'value2 1.12'}
hash:replace{1, 'value1 v1.32', 'value2 1.72'}
hash:replace{2, 'value1 v1.43', 'value2 1.92'}

-- Replace invalid fields
hash:replace{'invalid key', 'value1 v1.0', 'value2 v1.0'}

-------------------------------------------------------------------------------
-- 32-bit hash select fields test
-------------------------------------------------------------------------------

-- select by valid keys
hash.index['primary']:get{0}
hash.index['primary']:get{1}
hash.index['primary']:get{2}
hash.index['primary']:get{3}
hash.index['primary']:get{4}
hash.index['primary']:get{5}

-- select by invalid keys
hash.index['primary']:get{'invalid key'}
hash.index['primary']:get{1, 2}

-------------------------------------------------------------------------------
-- 32-bit hash delete fields test
-------------------------------------------------------------------------------

-- delete by valid keys
hash:delete{0}
hash:delete{1}
hash:delete{2}
hash:delete{3}
hash:delete{4}
hash:delete{5}

-- delete by invalid keys
hash:delete{'invalid key'}
hash:delete{1, 2}

hash:truncate()

--=============================================================================
-- 64-bit hash tests
--=============================================================================
-------------------------------------------------------------------------------
-- 64-bit hash inset fields tests
-------------------------------------------------------------------------------

-- Insert valid fields
hash:insert{0ULL, 'value1 v1.0', 'value2 v1.0'}
hash:insert{1ULL, 'value1 v1.0', 'value2 v1.0'}
hash:insert{2ULL, 'value1 v1.0', 'value2 v1.0'}
hash:insert{3ULL, 'value1 v1.0', 'value2 v1.0'}

-- Insert invalid fields
hash:insert{100, 'value1 v1.0', 'value2 v1.0'}
hash:insert{101, 'value1 v1.0', 'value2 v1.0'}
hash:insert{102, 'value1 v1.0', 'value2 v1.0'}
hash:insert{103, 'value1 v1.0', 'value2 v1.0'}
hash:insert{'invalid key', 'value1 v1.0', 'value2 v1.0'}

-------------------------------------------------------------------------------
-- 64-bit hash replace fields tests
-------------------------------------------------------------------------------

-- Replace valid fields
hash:replace{3ULL, 'value1 v1.31', 'value2 1.12'}
hash:replace{1ULL, 'value1 v1.32', 'value2 1.72'}
hash:replace{2ULL, 'value1 v1.43', 'value2 1.92'}

-- Replace invalid fields
hash:replace{3, 'value1 v1.31', 'value2 1.12'}
hash:replace{1, 'value1 v1.32', 'value2 1.72'}
hash:replace{2, 'value1 v1.43', 'value2 1.92'}
hash:replace{'invalid key', 'value1 v1.0', 'value2 v1.0'}

-------------------------------------------------------------------------------
-- 64-bit hash select fields test
-------------------------------------------------------------------------------

-- select by valid keys
hash.index['primary']:get{0ULL}
hash.index['primary']:get{1ULL}
hash.index['primary']:get{2ULL}
hash.index['primary']:get{3ULL}
hash.index['primary']:get{4ULL}
hash.index['primary']:get{5ULL}

-- select by valid NUM keys
hash.index['primary']:get{0}
hash.index['primary']:get{1}
hash.index['primary']:get{2}
hash.index['primary']:get{3}
hash.index['primary']:get{4}
hash.index['primary']:get{5}

-- select by invalid keys
hash.index['primary']:get{'invalid key'}
hash.index['primary']:get{'00000001', '00000002'}

-------------------------------------------------------------------------------
-- 64-bit hash delete fields test
-------------------------------------------------------------------------------

-- delete by valid keys
hash:delete{0ULL}
hash:delete{1ULL}
hash:delete{2ULL}
hash:delete{3ULL}
hash:delete{4ULL}
hash:delete{5ULL}

hash:insert{0ULL, 'value1 v1.0', 'value2 v1.0'}
hash:insert{1ULL, 'value1 v1.0', 'value2 v1.0'}
hash:insert{2ULL, 'value1 v1.0', 'value2 v1.0'}
hash:insert{3ULL, 'value1 v1.0', 'value2 v1.0'}

-- delete by valid NUM keys
hash:delete{0}
hash:delete{1}
hash:delete{2}
hash:delete{3}
hash:delete{4}
hash:delete{5}

-- delete by invalid keys
hash:delete{'invalid key'}
hash:delete{'00000001', '00000002'}
hash:truncate()

--=============================================================================
-- String hash tests
--=============================================================================
-------------------------------------------------------------------------------
-- String hash inset fields tests
-------------------------------------------------------------------------------
hash.index['primary']:drop()
tmp = hash:create_index('primary', { type = 'hash', parts = {1, 'string'}, unique = true })

-- Insert valid fields
hash:insert{'key 0', 'value1 v1.0', 'value2 v1.0'}
hash:insert{'key 1', 'value1 v1.0', 'value2 v1.0'}
hash:insert{'key 2', 'value1 v1.0', 'value2 v1.0'}
hash:insert{'key 3', 'value1 v1.0', 'value2 v1.0'}

-------------------------------------------------------------------------------
-- String hash replace fields tests
-------------------------------------------------------------------------------

-- Replace valid fields
hash:replace{'key 3', 'value1 v1.31', 'value2 1.12'}
hash:replace{'key 1', 'value1 v1.32', 'value2 1.72'}
hash:replace{'key 2', 'value1 v1.43', 'value2 1.92'}

-------------------------------------------------------------------------------
-- String hash select fields test
-------------------------------------------------------------------------------

-- select by valid keys
hash.index['primary']:get{'key 0'}
hash.index['primary']:get{'key 1'}
hash.index['primary']:get{'key 2'}
hash.index['primary']:get{'key 3'}
hash.index['primary']:get{'key 4'}
hash.index['primary']:get{'key 5'}

-- select by invalid keys
hash.index['primary']:get{'key 1', 'key 2'}

-------------------------------------------------------------------------------
-- String hash delete fields test
-------------------------------------------------------------------------------

-- delete by valid keys
hash:delete{'key 0'}
hash:delete{'key 1'}
hash:delete{'key 2'}
hash:delete{'key 3'}
hash:delete{'key 4'}
hash:delete{'key 5'}

-- delete by invalid keys
hash:delete{'key 1', 'key 2'}
hash:truncate()

-------------------------------------------------------------------------------
-- Collation test
-------------------------------------------------------------------------------
hash.index['primary']:drop()
tmp = hash:create_index('primary', { type = 'hash', parts = {{1, 'string', collation = 'unicode_ci'}}, unique = true})
tmp = hash:create_index('secondary', { type = 'hash', parts = {{2, 'scalar', collation = 'unicode_ci'}}, unique = true})

hash:insert{'Ёж', 'Hedgehog'}
hash:insert{'Ёлка', 'Spruce'}
hash:insert{'Jogurt', 'Йогурт'}
hash:insert{'Один', 1}

hash.index.primary:get('ёж')
hash.index.primary:get('елка')
hash.index.secondary:get('spruce')
hash.index.secondary:get('йогурт')
hash.index.secondary:get(1)
hash.index.secondary:get('иогурт')
hash.index.secondary:get(2)

------------------------
-- hash::replace tests
------------------------
hash.index['secondary']:drop()
hash.index['primary']:drop()
tmp = hash:create_index('primary', { type = 'hash', parts = {1, 'unsigned'}, unique = true })
tmp = hash:create_index('field1', { type = 'hash', parts = {2, 'unsigned'}, unique = true })
tmp = hash:create_index('field2', { type = 'hash', parts = {3, 'unsigned'}, unique = true })
tmp = hash:create_index('field3', { type = 'hash', parts = {4, 'unsigned'}, unique = true })

hash:insert{0, 0, 0, 0}
hash:insert{1, 1, 1, 1}
hash:insert{2, 2, 2, 2}

-- OK
hash:replace{1, 1, 1, 1}
hash.index['primary']:get{10}
hash.index['field1']:get{10}
hash.index['field2']:get{10}
hash.index['field3']:get{10}
hash.index['primary']:get{1}
hash.index['field1']:get{1}
hash.index['field2']:get{1}
hash.index['field3']:get{1}

-- OK
hash:insert{10, 10, 10, 10}
hash:delete{10}
hash.index['primary']:get{10}
hash.index['field1']:get{10}
hash.index['field2']:get{10}
hash.index['field3']:get{10}

-- TupleFound (primary key)
hash:insert{1, 10, 10, 10}
hash.index['primary']:get{10}
hash.index['field1']:get{10}
hash.index['field2']:get{10}
hash.index['field3']:get{10}
hash.index['primary']:get{1}

-- TupleNotFound (primary key)
hash:replace{10, 10, 10, 10}
hash.index['primary']:get{10}
hash.index['field1']:get{10}
hash.index['field2']:get{10}
hash.index['field3']:get{10}

-- TupleFound (key --1)
hash:insert{10, 0, 10, 10}
hash.index['primary']:get{10}
hash.index['field1']:get{10}
hash.index['field2']:get{10}
hash.index['field3']:get{10}
hash.index['field1']:get{0}

-- TupleFound (key --1)
-- hash:replace_if_exists(2, 0, 10, 10)
hash.index['primary']:get{10}
hash.index['field1']:get{10}
hash.index['field2']:get{10}
hash.index['field3']:get{10}
hash.index['field1']:get{0}

-- TupleFound (key --3)
hash:insert{10, 10, 10, 0}
hash.index['primary']:get{10}
hash.index['field1']:get{10}
hash.index['field2']:get{10}
hash.index['field3']:get{10}
hash.index['field3']:get{0}

-- TupleFound (key --3)
-- hash:replace_if_exists(2, 10, 10, 0)
hash.index['primary']:get{10}
hash.index['field1']:get{10}
hash.index['field2']:get{10}
hash.index['field3']:get{10}
hash.index['field3']:get{0}

hash:drop()

hash = box.schema.space.create('tweedledum')
hi = hash:create_index('primary', { type = 'hash', parts = {1, 'unsigned'}, unique = true })
hash:insert{0}
hash:insert{16}
for _, tuple in hi:pairs(nil, {iterator = box.index.ALL}) do hash:delete{tuple[1]} end
hash:drop()

-- 
-- gh-616 "1-based indexing and 0-based error message
--
_ = box.schema.create_space('test')
_ = box.space.test:create_index('i',{parts={1,'string'}})
box.space.test:insert{1}
box.space.test:drop()

-- gh-1467: invalid iterator type
space = box.schema.space.create('test')
index = space:create_index('primary', { type = 'hash' })
space:select({1}, {iterator = 'BITS_ALL_SET' } )
space:drop()

-- gh-3907: check that integer numbers stored as MP_FLOAT/MP_DOUBLE
-- are hashed as MP_INT/MP_UINT.
ffi = require('ffi')
s = box.schema.space.create('test')
_ = s:create_index('primary', {type = 'hash', parts = {1, 'number'}})
s:insert{ffi.new('double', 0)}
s:insert{ffi.new('double', -1)}
s:insert{ffi.new('double', 9007199254740992)}
s:insert{ffi.new('double', -9007199254740994)}
s:get(0LL)
s:get(-1LL)
s:get(9007199254740992LL)
s:get(-9007199254740994LL)
s:drop()
