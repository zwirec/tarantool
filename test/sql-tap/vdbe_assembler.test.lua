#!/usr/bin/env tarantool
test = require("sqltester")
test:plan(6)

-- Make sure that simple inserts, deletes and selects work as desired.
--

test:do_execsql_test(
    "vdbe-asm-1.1",
    [[
	create table t1(id primary key, a);
	insert into t1 values(1, null);
	insert into t1 values(2, 123);
	insert into t1 values(3, -193);
	insert into t1 values(4, 'Hello, World!');
	insert into t1 values(5, 3872.12);
	insert into t1 values(6, 0);
	pragma vdbe_dump_file = 'select.vdbe';
	pragma vdbe_dump = 1;
	select * from t1;
	pragma vdbe_dump = 0;
	pragma vdbe_execute = 'select.vdbe';
    ]], {
        -- <vdbe-asm-1.1>
        -- <vdbe-asm-1.1>
    })

test:do_execsql_test(
    "vdbe-asm-1.2",
    [[
	pragma vdbe_dump_file = 'delete.vdbe';
	pragma vdbe_dump = 1;
	delete from t1 where id = 4 or id = 3;
	pragma vdbe_dump = 0;
	insert into t1 values (3, 666), (4, 'Hello again!');
	pragma vdbe_execute = 'delete.vdbe';
	pragma vdbe_execute = 'select.vdbe';
    ]], {
        -- <vdbe-asm-1.2>
        -- <vdbe-asm-1.2>
    })

test:do_execsql_test(
    "vdbe-asm-1.3",
    [[
	pragma vdbe_dump_file = 'insert.vdbe';
	pragma vdbe_dump = 1;
	insert into t1 values (3, 666), (4, 'Hello again!');
	pragma vdbe_dump = 0;
	pragma vdbe_execute = 'delete.vdbe';
	pragma vdbe_execute = 'insert.vdbe';
	pragma vdbe_execute = 'select.vdbe';
    ]], {
        -- <vdbe-asm-1.3>
        -- <vdbe-asm-1.3>
    })

-- A bit more sophisticated select.
test:do_execsql_test(
    "vdbe-asm-1.4",
    [[
	pragma vdbe_dump_file = 'select.vdbe';
	pragma vdbe_dump = 1;
	select a from t1 where id = 2 or a = 666;
	pragma vdbe_dump = 0;
	pragma vdbe_execute = 'select.vdbe';
    ]], {
        -- <vdbe-asm-1.4>
        -- <vdbe-asm-1.4>
    })

-- Make sure that truncate works as well.
test:do_execsql_test(
    "vdbe-asm-1.5",
    [[
	pragma vdbe_dump_file = 'delete.vdbe';
	pragma vdbe_dump = 1;
	delete from t1;
	pragma vdbe_dump_file = 'select.vdbe';
	select * from t1;
	pragma vdbe_dump = 0;
	insert into t1 values(1, null);
	insert into t1 values(2, 123);
	insert into t1 values(3, -193);
	insert into t1 values(4, 'Hello, World!');
	insert into t1 values(5, 3872.12);
	insert into t1 values(6, 0);
	pragma vdbe_execute = 'delete.vdbe';
	pragma vdbe_execute = 'select.vdbe';
    ]], {
        -- <vdbe-asm-1.5>
        -- <vdbe-asm-1.5>
    })

test:do_execsql_test(
    "vdbe-asm-1.6",
    [[
	CREATE TABLE songs(songid primary key, artist, timesplayed);
	INSERT INTO songs VALUES(1,'one',1);
	INSERT INTO songs VALUES(2,'one',2);
	INSERT INTO songs VALUES(3,'two',3);
	INSERT INTO songs VALUES(4,'three',5);
	INSERT INTO songs VALUES(5,'one',7);
	INSERT INTO songs VALUES(6,'two',11);
	pragma vdbe_dump_file = 'select.vdbe';
	pragma vdbe_dump = 1;
	SELECT DISTINCT artist,timesplayed AS total FROM songs GROUP BY artist LIMIT 1 OFFSET 1;
	pragma vdbe_dump = 0;
	pragma vdbe_execute = 'select.vdbe';
    ]], {
        -- <vdbe-asm-1.6>
        -- <vdbe-asm-1.6>
    })


os.remove("select.vdbe")
os.remove("delete.vdbe")
os.remove("insert.vdbe")

test:finish_test()

