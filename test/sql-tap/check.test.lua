#!/usr/bin/env tarantool
test = require("sqltester")
test:plan(51)

--!./tcltestrunner.lua
-- 2005 November 2
--
-- The author disclaims copyright to this source code.  In place of
-- a legal notice, here is a blessing:
--
--    May you do good and not evil.
--    May you find forgiveness for yourself and forgive others.
--    May you share freely, never taking more than you give.
--
-------------------------------------------------------------------------
-- This file implements regression tests for SQLite library.  The
-- focus of this file is testing CHECK constraints
--
-- $Id: check.test,v 1.13 2009/06/05 17:09:12 drh Exp $
-- ["set","testdir",[["file","dirname",["argv0"]]]]
-- ["source",[["testdir"],"\/tester.tcl"]]
testprefix = "check"
-- Only run these tests if the build includes support for CHECK constraints



test:do_execsql_test(
    "check-1.1",
    [[
        CREATE TABLE t1(
          x INTEGER CHECK( x<5 ),
          y REAL CHECK( y>x ),
          z  INT primary key
        );
    ]], {
        -- <check-1.1>

        -- </check-1.1>
    })

test:do_execsql_test(
    "check-1.2",
    [[
        INSERT INTO t1 VALUES(3,4, 1);
        SELECT x, y FROM t1;
    ]], {
        -- <check-1.2>
        3, 4.0
        -- </check-1.2>
    })

test:do_catchsql_test(
    "check-1.3",
    [[
        INSERT INTO t1 VALUES(6,7, 2);
    ]], {
        -- <check-1.3>
        1, "Failed to execute SQL statement: CHECK constraint failed: T1"
        -- </check-1.3>
    })

test:do_execsql_test(
    "check-1.4",
    [[
        SELECT x, y FROM t1;
    ]], {
        -- <check-1.4>
        3, 4.0
        -- </check-1.4>
    })

test:do_catchsql_test(
    "check-1.5",
    [[
        INSERT INTO t1 VALUES(4,3, 2);
    ]], {
        -- <check-1.5>
        1, "Failed to execute SQL statement: CHECK constraint failed: T1"
        -- </check-1.5>
    })

test:do_execsql_test(
    "check-1.6",
    [[
        SELECT x, y FROM t1;
    ]], {
        -- <check-1.6>
        3, 4.0
        -- </check-1.6>
    })

test:do_catchsql_test(
    "check-1.7",
    [[
        INSERT INTO t1 VALUES(NULL,6, 4);
    ]], {
        -- <check-1.7>
        0
        -- </check-1.7>
    })

test:do_execsql_test(
    "check-1.8",
    [[
        SELECT x, y FROM t1;
    ]], {
        -- <check-1.8>
        3, 4.0, "", 6.0
        -- </check-1.8>
    })

test:do_catchsql_test(
    "check-1.9",
    [[
        INSERT INTO t1 VALUES(2,NULL, 5);
    ]], {
        -- <check-1.9>
        0
        -- </check-1.9>
    })

test:do_execsql_test(
    "check-1.10",
    [[
        SELECT x, y FROM t1;
    ]], {
        -- <check-1.10>
        3, 4.0, "", 6.0, 2, ""
        -- </check-1.10>
    })

test:do_execsql_test(
    "check-1.11",
    [[
        DELETE FROM t1 WHERE x IS NULL OR x!=3;
        UPDATE t1 SET x=2 WHERE x==3;
        SELECT x, y FROM t1;
    ]], {
        -- <check-1.11>
        2, 4.0
        -- </check-1.11>
    })

test:do_catchsql_test(
    "check-1.12",
    [[
        UPDATE t1 SET x=7 WHERE x==2
    ]], {
        -- <check-1.12>
        1, "Failed to execute SQL statement: CHECK constraint failed: T1"
        -- </check-1.12>
    })

test:do_execsql_test(
    "check-1.13",
    [[
        SELECT x, y FROM t1;
    ]], {
        -- <check-1.13>
        2, 4.0
        -- </check-1.13>
    })

test:do_catchsql_test(
    "check-1.14",
    [[
        UPDATE t1 SET x=5 WHERE x==2
    ]], {
        -- <check-1.14>
        1, "Failed to execute SQL statement: CHECK constraint failed: T1"
        -- </check-1.14>
    })

test:do_execsql_test(
    "check-1.15",
    [[
        SELECT x, y FROM t1;
    ]], {
        -- <check-1.15>
        2, 4.0
        -- </check-1.15>
    })

test:do_catchsql_test(
    "check-1.16",
    [[
        UPDATE t1 SET x=4, y=11 WHERE x==2
    ]], {
        -- <check-1.16>
        0
        -- </check-1.16>
    })

test:do_execsql_test(
    "check-1.17",
    [[
        SELECT x, y FROM t1;
    ]], {
        -- <check-1.17>
        4, 11.0
        -- </check-1.17>
    })

test:do_catchsql_test(
    "check-2.1",
    [[
        CREATE TABLE t2(
          id  INT primary key,
          x INTEGER CONSTRAINT one CHECK( typeof(coalesce(x,0))=='integer'),
          y REAL CONSTRAINT two CHECK( typeof(coalesce(y,0.1))=='real' ),
          z TEXT CONSTRAINT three CHECK( typeof(coalesce(z,''))=='text' )
        );
    ]], {
        -- <check-2.1>
        1, "SQL error: TYPEOF function is forbidden for usage in Check constraint"
        -- </check-2.1>
    })

test:do_execsql_test(
    "check-2.cleanup",
    [[
        DROP TABLE IF EXISTS t2b;
        DROP TABLE IF EXISTS t2c;
    ]], {
        -- <check-2.cleanup>

        -- </check-2.cleanup>
    })

test:do_catchsql_test(
    "check-3.1",
    [[
        CREATE TABLE t3(
          x  INT primary key, y INT , z INT ,
          CHECK( x<(SELECT min(x) FROM t1) )
        );
    ]], {
        -- <check-3.1>
        1, "Failed to create space 'T3': SQL error: subqueries prohibited in CHECK constraints"
        -- </check-3.1>
    })


--    MUST_WORK_TEST use smth instead of sqlite_master
--    test:do_execsql_test(
--        "check-3.2",
--        [[
--            SELECT name FROM sqlite_master ORDER BY name
--        ]], {
--            -- <check-3.2>
--            "t1", "t2"
--            -- </check-3.2>
--        })

test:do_catchsql_test(
    "check-3.3",
    [[
        CREATE TABLE t3(
          x  INT primary key, y INT , z INT ,
          CHECK( q<x )
        );
    ]], {
        -- <check-3.3>
        1, "Failed to create space 'T3': SQL error: no such column: Q"
        -- </check-3.3>
    })

--    MUST_WORK_TEST use smth instead of sqlite_master
--    test:do_execsql_test(
--        "check-3.4",
--        [[
--            SELECT name FROM sqlite_master ORDER BY name
--        ]], {
--            -- <check-3.4>
--            "t1", "t2"
--            -- </check-3.4>
--        })

test:do_catchsql_test(
    "check-3.5",
    [[
        CREATE TABLE t3(
          x  INT primary key, y INT , z INT ,
          CHECK( t2.x<x )
        );
    ]], {
        -- <check-3.5>
        1, "Failed to create space 'T3': SQL error: no such column: T2.X"
        -- </check-3.5>
    })

--    MUST_WORK_TEST use smth instead of sqlite_master
--    test:do_execsql_test(
--        "check-3.6",
--        [[
--            SELECT name FROM sqlite_master ORDER BY name
--        ]], {
--            -- <check-3.6>
--            "t1", "t2"
--            -- </check-3.6>
--        })

test:do_catchsql_test(
    "check-3.7",
    [[
        CREATE TABLE t3(
          x  INT primary key, y INT , z INT ,
          CHECK( t3.x<25 )
        );
    ]], {
        -- <check-3.7>
        0
        -- </check-3.7>
    })

test:do_execsql_test(
    "check-3.8",
    [[
        INSERT INTO t3 VALUES(1,2,3);
        SELECT * FROM t3;
    ]], {
        -- <check-3.8>
        1, 2, 3
        -- </check-3.8>
    })

test:do_catchsql_test(
    "check-3.9",
    [[
        INSERT INTO t3 VALUES(111,222,333);
    ]], {
        -- <check-3.9>
        1, "Failed to execute SQL statement: CHECK constraint failed: T3"
        -- </check-3.9>
    })

test:do_execsql_test(
    "check-4.1",
    [[
        CREATE TABLE t4(x INT UNIQUE, y INT, z INT PRIMARY KEY
          CHECK (
               x+y==11
            OR x*y==12
            OR x/y BETWEEN 5 AND 8
            OR -x==y+10
          )
        );
    ]], {
        -- <check-4.1>

        -- </check-4.1>
    })

test:do_execsql_test(
    "check-4.2",
    [[
        INSERT INTO t4 VALUES(1,10,1);
        SELECT x,y FROM t4
    ]], {
        -- <check-4.2>
        1, 10
        -- </check-4.2>
    })

test:do_execsql_test(
    "check-4.3",
    [[
        UPDATE t4 SET x=4, y=3;
        SELECT x,y FROM t4
    ]], {
        -- <check-4.3>
        4, 3
        -- </check-4.3>
    })

test:do_execsql_test(
    "check-4.4",
    [[
        UPDATE t4 SET x=12, y=2;
        SELECT x,y FROM t4
    ]], {
        -- <check-4.4>
        12, 2
        -- </check-4.4>
    })

test:do_execsql_test(
    "check-4.5",
    [[
        UPDATE t4 SET x=12, y=-22;
        SELECT x,y FROM t4
    ]], {
        -- <check-4.5>
        12, -22
        -- </check-4.5>
    })

test:do_catchsql_test(
    "check-4.6",
    [[
        UPDATE t4 SET x=0, y=1;
    ]], {
        -- <check-4.6>
        1, "Failed to execute SQL statement: CHECK constraint failed: T4"
        -- </check-4.6>
    })

test:do_execsql_test(
    "check-4.7",
    [[
        SELECT x,y FROM t4;
    ]], {
        -- <check-4.7>
        12, -22
        -- </check-4.7>
    })

test:do_catchsql_test(
    "check-4.9",
    [[
        UPDATE t4 SET x=0, y=2;
    ]], {
        -- <check-4.9>
        1, "Failed to execute SQL statement: CHECK constraint failed: T4"
        -- </check-4.9>
    })

test:do_catchsql_test(
    "check-5.1",
    [[
        CREATE TABLE t5(x  INT primary key, y INT ,
          CHECK( x*y<:abc )
        );
    ]], {
        -- <check-5.1>
        1, "Wrong space options (field 5): invalid expression specified (SQL error: bindings are not allowed in DDL)"
        -- </check-5.1>
    })

test:do_catchsql_test(
    "check-5.2",
    [[
        CREATE TABLE t5(x  INT primary key, y INT ,
          CHECK( x*y<? )
        );
    ]], {
        -- <check-5.2>
        1, "Wrong space options (field 5): invalid expression specified (SQL error: bindings are not allowed in DDL)"
        -- </check-5.2>
    })

test:do_execsql_test(
    "check-6.1",
    [[
        SELECT x, y FROM t1
    ]], {
        -- <check-6.1>
        4, 11.0
        -- </check-6.1>
    })

test:do_execsql_test(
    "check-6.2",
    [[
        UPDATE OR IGNORE t1 SET x=5;
        SELECT x, y FROM t1;
    ]], {
        -- <check-6.2>
        4, 11.0
        -- </check-6.2>
    })

test:do_execsql_test(
    "check-6.3",
    [[
        INSERT OR IGNORE INTO t1 VALUES(5,4.0, 7);
        SELECT x, y FROM t1;
    ]], {
        -- <check-6.3>
        4, 11.0
        -- </check-6.3>
    })

test:do_execsql_test(
    "check-6.4",
    [[
        INSERT OR IGNORE INTO t1 VALUES(2,20.0, 8);
        SELECT x, y FROM t1;
    ]], {
        -- <check-6.4>
        4, 11.0, 2, 20.0
        -- </check-6.4>
    })

test:do_catchsql_test(
    "check-6.5",
    [[
        UPDATE OR FAIL t1 SET x=7-x, y=y+1;
    ]], {
        -- <check-6.5>
        1, "Failed to execute SQL statement: CHECK constraint failed: T1"
        -- </check-6.5>
    })

test:do_execsql_test(
    "check-6.6",
    [[
        SELECT x, y FROM t1;
    ]], {
        -- <check-6.6>
        3, 12.0, 2, 20.0
        -- </check-6.6>
    })

test:do_catchsql_test(
    "check-6.7",
    [[
        START TRANSACTION;
        INSERT INTO t1 VALUES(1,30.0, 9);
        INSERT OR ROLLBACK INTO t1 VALUES(8,40.0, 10);
    ]], {
        -- <check-6.7>
        1, "Failed to execute SQL statement: CHECK constraint failed: T1"
        -- </check-6.7>
    })

test:do_catchsql_test(
    "check-6.8",
    [[
        COMMIT;
    ]], {
        -- <check-6.8>
        1, "cannot commit - no transaction is active"
        -- </check-6.8>
    })

-- MUST_WORK_TEST rollback
-- values 1,30.0 should not be presented in t1 after 6.7 test
test:execsql("DELETE FROM t1 where z = 9;")
--    test:do_execsql_test(
--        "check-6.9",
--        [[
--            SELECT x, y FROM t1
--        ]], {
--            -- <check-6.9>
--            3, 12.0, 2, 20.0
--            -- </check-6.9>
--        })

test:do_catchsql_test(
    "check-6.12",
    [[
        REPLACE INTO t1 VALUES(6,7, 11);
    ]], {
        -- <check-6.12>
        1, "Failed to execute SQL statement: CHECK constraint failed: T1"
        -- </check-6.12>
    })

test:do_execsql_test(
    "check-6.13",
    [[
        SELECT x, y FROM t1
    ]], {
        -- <check-6.13>
        3, 12.0, 2, 20.0
        -- </check-6.13>
    })

test:do_catchsql_test(
    "check-6.14",
    [[
        INSERT OR IGNORE INTO t1 VALUES(6,7,12);
    ]], {
        -- <check-6.14>
        0
        -- </check-6.14>
    })

test:do_execsql_test(
    "check-6.15",
    [[
        SELECT x, y FROM t1
    ]], {
        -- <check-6.15>
        3, 12.0, 2, 20.0
        -- </check-6.15>
    })



----------------------------------------------------------------------------
-- If a connection opens a database that contains a CHECK constraint that
-- uses an unknown UDF, the schema should not be considered malformed.
-- Attempting to modify the table should fail (since the CHECK constraint
-- cannot be tested).
--
--reset_db()
local function myfunc(x)
    return x < 10
end
box.internal.sql_create_function("myfunc", "INT", myfunc)

test:do_execsql_test(
    7.1,
    [[
        CREATE TABLE t6(a  INT CHECK (myfunc(a)) primary key)
    ]])

test:do_execsql_test(
    7.2,
    [[
        INSERT INTO t6 VALUES(9)
    ]])

test:do_catchsql_test(
    7.3,
    " INSERT INTO t6 VALUES(11) ", {
        -- <7.3>
        1, "Failed to execute SQL statement: CHECK constraint failed: T6"
        -- </7.3>
    })

--MUST_WORK_TEST tarantool should be properly restarted
if (0 > 0) then
test:do_test(
    7.4,
    function()
        --sqlite3("db2", "test.db")
        return test:execsql(" SELECT * FROM t6 ") --, "db2")
    end, {
        -- <7.4>
        9
        -- </7.4>
    })

test:do_test(
    7.5,
    function()
        return test:catchsql(" INSERT INTO t6 VALUES(8) ") --, "db2")
    end, {
        -- <7.5>
        1, "unknown function: MYFUNC()"
        -- </7.5>
    })

test:do_test(
    7.6,
    function()
        return test:catchsql(" CREATE TABLE t7(a  INT CHECK (myfunc(a))) ") --, "db2 INT ")
    end, {
        -- <7.6>
        1, "no such function: MYFUNC"
        -- </7.6>
    })

test:do_test(
    7.7,
    function()
        --db2("func", "myfunc", "myfunc")
        return test:execsql(" INSERT INTO t6 VALUES(8) ") --, "db2")
    end, {
        -- <7.7>

        -- </7.7>
    })

test:do_test(
    7.8,
    function()
        --db2("func", "myfunc", "myfunc")
        return test:catchsql(" INSERT INTO t6 VALUES(12) ", "db2")
    end, {
        -- <7.8>
        1, "Failed to execute SQL statement: CHECK constraint failed: T6"
        -- </7.8>
    })
end

-- 2013-08-02:  Silently ignore database name qualifiers in CHECK constraints.
--
test:do_execsql_test(
    8.1,
    [[
        CREATE TABLE t810(a  INT primary key, CHECK( t810.a>0 ));
    ]], {
        -- <8.1>

        -- </8.1>
    })

test:finish_test()

