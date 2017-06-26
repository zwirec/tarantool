-- Tests for HASH index type

s3 = box.schema.space.create('my_space4')
i3_1 = s3:create_index('my_space4_idx1', {type='HASH', parts={1, 'scalar', 2, 'integer', 3, 'number'}, unique=true})
i3_2 = s3:create_index('my_space4_idx2', {type='HASH', parts={4, 'string', 5, 'scalar'}, unique=true})
s3:insert({100.5, 30, 95, "str1", 5})
s3:insert({"abc#$23", 1000, -21.542, "namesurname", 99})
s3:insert({true, -459, 4000, "foobar", "36.6"})
s3:select{}

i3_1:select({100.5})
i3_1:select({true, -459})
i3_1:select({"abc#$23", 1000, -21.542})

i3_2:select({"str1", 5})
i3_2:select({"str"})
i3_2:select({"str", 5})
i3_2:select({"foobar", "36.6"})

s3:drop()

-- #2112 int vs. double compare
s5 = box.schema.space.create('my_space5')
_ = s5:create_index('primary', {parts={1, 'scalar'}})
-- small range 1
s5:insert({5})
s5:insert({5.1})
s5:select()
s5:truncate()
-- small range 2
s5:insert({5.1})
s5:insert({5})
s5:select()
s5:truncate()
-- small range 3
s5:insert({-5})
s5:insert({-5.1})
s5:select()
s5:truncate()
-- small range 4
s5:insert({-5.1})
s5:insert({-5})
s5:select()
s5:truncate()
-- conversion to another type is lossy for both values
s5:insert({18446744073709551615ULL})
s5:insert({3.6893488147419103e+19})
s5:select()
s5:truncate()
-- insert in a different order to excersise another codepath
s5:insert({3.6893488147419103e+19})
s5:insert({18446744073709551615ULL})
s5:select()
s5:truncate()
-- MP_INT vs MP_UINT
s5:insert({-9223372036854775808LL})
s5:insert({-3.6893488147419103e+19})
s5:select()
s5:truncate()
-- insert in a different order to excersise another codepath
s5:insert({-3.6893488147419103e+19})
s5:insert({-9223372036854775808LL})
s5:select()
s5:truncate()
-- different signs 1
s5:insert({9223372036854775807LL})
s5:insert({-3.6893488147419103e+19})
s5:select()
s5:truncate()
-- different signs 2
s5:insert({-3.6893488147419103e+19})
s5:insert({9223372036854775807LL})
s5:select()
s5:truncate()
-- different signs 3
s5:insert({-9223372036854775808LL})
s5:insert({3.6893488147419103e+19})
s5:select()
s5:truncate()
-- different signs 4
s5:insert({3.6893488147419103e+19})
s5:insert({-9223372036854775808LL})
s5:select()
s5:truncate()
-- different magnitude 1
s5:insert({1.1})
s5:insert({18446744073709551615ULL})
s5:select()
s5:truncate()
-- different magnitude 2
s5:insert({18446744073709551615ULL})
s5:insert({1.1})
s5:select()
s5:truncate()
-- Close values
ffi = require('ffi')
ffi.new('double', 1152921504606846976) == 1152921504606846976ULL
ffi.new('double', 1152921504606846977) == 1152921504606846976ULL
-- Close values 1
s5:insert({1152921504606846976ULL})
s5:insert({ffi.new('double', 1152921504606846976ULL)}) -- fail
s5:select()
s5:truncate()
-- Close values 2
s5:insert({1152921504606846977ULL})
s5:insert({ffi.new('double', 1152921504606846976ULL)}) -- success
s5:select()
s5:truncate()
-- Close values 3
s5:insert({-1152921504606846976LL})
s5:insert({ffi.new('double', -1152921504606846976LL)}) -- fail
s5:select()
s5:truncate()
-- Close values 4
s5:insert({-1152921504606846977LL})
s5:insert({ffi.new('double', -1152921504606846976LL)}) -- success
s5:select()
s5:truncate()
-- Close values 5
ffi.cdef "double exp2(double);"
s5:insert({0xFFFFFFFFFFFFFFFFULL})
s5:insert({ffi.new('double', ffi.C.exp2(64))}) -- success
s5:select()
s5:truncate()
-- Close values 6
s5:insert({0x8000000000000000LL})
s5:insert({ffi.new('double', -ffi.C.exp2(63))}) -- duplicate
s5:select()
s5:truncate()
-- Close values 7
s5:insert({0x7FFFFFFFFFFFFFFFLL})
s5:insert({ffi.new('double', ffi.C.exp2(63))}) -- ok
s5:select()
s5:truncate()

s5:drop()

-- partial index
-- tree index
s = box.schema.space.create('test')
i1 = s:create_index('i1', {type = 'tree', parts = {1, 'uint'}, partial = true}) -- error
i1 = s:create_index('i1', {type = 'hash', parts = {1, 'uint'}, partial = true}) -- error
i1 = s:create_index('i1', {type = 'tree', parts = {1, 'uint'}})
i2 = s:create_index('i2', {type = 'tree', parts = {2, 'uint'}, partial = true})

s:insert{1, 2}
s:insert{4, 3}
s:insert{8, 0}
i2:select{}
i2:select{2}
i2:select{3}
i2:select{0}

s:replace{box.NULL, 0} -- error
s:replace{8, box.NULL, 18}

i1:select{}
i2:select{}
i2:select{2}
i2:select{3}
i2:select{0}

s:replace{9, box.NULL, 19 }
i1:select{}
i2:select{}

s:delete{1}
s:delete{8}
i1:select{}
i2:select{}
collectgarbage('collect')
i1:select{}
i2:select{}

s:drop()

-- hash index
s = box.schema.space.create('test')
i1 = s:create_index('i1', {type = 'tree', parts = {1, 'uint'}})
i2 = s:create_index('i2', {type = 'hash', parts = {2, 'uint'}, partial = true})

s:insert{1, 2}
s:insert{4, 3}
s:insert{8, 0}
i2:select{}
i2:select{2}
i2:select{3}
i2:select{0}

s:replace{8, box.NULL, 18}

i1:select{}
i2:select{}
i2:select{2}
i2:select{3}
i2:select{0}

s:replace{9, box.NULL, 19 }
i1:select{}
i2:select{}

s:delete{1}
s:delete{8}
i1:select{}
i2:select{}
collectgarbage('collect')
i1:select{}
i2:select{}

s:drop()

-- rtree index
s = box.schema.space.create('test')
i1 = s:create_index('i1', {type = 'tree', parts = {1, 'uint'}})
i2 = s:create_index('i2', {type = 'rtree', parts = {2, 'array'}, partial = true})

s:insert{1, box.NULL, 2}
s:insert{2, {1, 1, 2, 2}, 3}
s:insert{3, box.NULL, 4}
s:insert{4, {2, 2, 3, 3}, 5}

i1:select{}
i2:select{}
i2:select{1, 1, 2, 2}

s:delete{1}
s:delete{2}

i1:select{}
i2:select{}

s:drop()

-- bitset index
s = box.schema.space.create('test')
i1 = s:create_index('i1', {type = 'tree', parts = {1, 'uint'}})
i2 = s:create_index('i2', {type = 'bitset', parts = {2, 'uint'}, partial = true})

s:insert{1, box.NULL, 2}
s:insert{2, 1, 3}
s:insert{3, box.NULL, 4}
s:insert{4, 3, 5}

i1:select{}
i2:select{}
i2:select{1}

s:delete{1}
s:delete{2}

i1:select{}
i2:select{}
i2:select{1}

s:drop()

-- several indexes
s = box.schema.space.create('test')
i1 = s:create_index('i1', {type = 'tree', parts = {1, 'uint', 2, 'uint'}})
i2 = s:create_index('i2', {type = 'tree', parts = {2, 'uint', 3, 'uint'}, partial = true})
i3 = s:create_index('i3', {type = 'tree', parts = {5, 'uint'}, unique = false})
i4 = s:create_index('i4', {type = 'tree', parts = {3, 'uint', 4, 'uint', 5, 'uint'}, partial = true})

s:insert{1, 2, 3, 4, 5 }
s:insert{2, box.NULL, 4, 5, 6} -- fail
s:insert{3, 4, box.NULL, 6, 7} -- ok
s:insert{4, 5, 6, box.NULL, 7} -- ok
s:insert{6, 7, 8, 9, box.NULL, 11} -- fail

i1:select{}
i2:select{}
i3:select{}
i4:select{}

s:delete{1, 2}
s:delete{3, 4}

i1:select{}
i2:select{}
i3:select{}
i4:select{}

s:drop()

-- vinyl
s = box.schema.space.create('test', {engine = 'vinyl'})
i1 = s:create_index('i1', {type = 'tree', parts = {1, 'uint'}})
i2 = s:create_index('i2', {type = 'hash', parts = {2, 'uint'}, partial = true})
s:drop()

