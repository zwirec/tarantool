s, err = pcall(box.error, 1, "err")
err.type
err.trace[12].file
err.trace[12].line
#err.trace

s, err = pcall(error, "oh no" )
err = err:unpack()
err.type
err.message
#err.trace
nil_var=nil
s, err = pcall(function() return nil_var.b end)

function fail() return nil_var.b end

s, err = pcall(fail)
err = err:unpack()
err.type
err.message
#err.trace

box.begin()
s, err = pcall(box.schema.user.create, "user" )
err = err:unpack()
err.type
err.message
#err.trace

s = box.schema.space.create("space")
_ = s:create_index("prim")
_ =s:on_replace(fail)
st,err = pcall(s.insert, s, {1})
err= err:unpack()
err.type
err.message
#err.trace
s:drop()

errinj = box.error.injection
space = box.schema.space.create('tweedledum')
index = space:create_index('primary', { type = 'hash' })
errinj.set("ERRINJ_TESTING", true)
s, err = pcall(space.select, space, {222444})
errinj.set("ERRINJ_TESTING", false)
err = err:unpack()
err.type
err.message
#err.trace
space:drop()
