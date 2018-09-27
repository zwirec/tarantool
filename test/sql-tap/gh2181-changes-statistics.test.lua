#!/usr/bin/env tarantool
test = require("sqltester")
test:plan(56)

test:do_select_tests(
	"gh2181-changes-statistics",
	{
		{"0.1", "SELECT CHANGES()", {0}},
		{"0.2", "SELECT TOTAL_CHANGES()", {0}},
		{"1.0", "CREATE TABLE T1(A PRIMARY KEY)", {}},
		{"1.1", "SELECT CHANGES()", {1}},
		{"1.2", "SELECT TOTAL_CHANGES()", {1}},
		{"2.0", "INSERT INTO T1 VALUES(1)", {}},
		{"2.1", "SELECT CHANGES()", {1}},
		{"2.2", "SELECT TOTAL_CHANGES()", {2}},
		{"3.0", "INSERT INTO T1 VALUES(2)", {}},
		{"3.1", "SELECT CHANGES()", {1}},
		{"3.2", "SELECT TOTAL_CHANGES()", {3}},
		{"4.0", "CREATE TABLE T2(A PRIMARY KEY)", {}},
		{"4.1", "SELECT CHANGES()", {1}},
		{"4.2", "SELECT TOTAL_CHANGES()", {4}},
		{"5.0", "INSERT INTO T2 SELECT * FROM T1", {}},
		{"5.1", "SELECT CHANGES()", {2}},
		{"5.2", "SELECT TOTAL_CHANGES()", {6}},
		{"6.0", [[
			CREATE TRIGGER TRIG1 AFTER INSERT ON T1 FOR EACH ROW BEGIN
                INSERT INTO T2 VALUES(NEW.A);
            END;
            ]],{}},
		{"6.1", "SELECT CHANGES()", {1}},
		{"6.2", "SELECT TOTAL_CHANGES()", {7}},
		{"6.3", "INSERT INTO T1 VALUES(3)", {}},
        -- 1 instead of 2; sqlite is the same.
		{"6.4", "SELECT CHANGES()", {1}},
		{"6.5", "SELECT TOTAL_CHANGES()", {9}},
		{"6.6", "DROP TRIGGER TRIG1"},
		{"6.7", "SELECT CHANGES()", {1}},
		{"6.8", "SELECT TOTAL_CHANGES()", {10}},
		{"7.0", "DELETE FROM T1 WHERE A = 1", {}},
		{"7.1", "SELECT CHANGES()", {1}},
		{"7.2", "SELECT TOTAL_CHANGES()", {11}},
		{"8.0", "UPDATE T1 SET A = 1 where A = 2", {}},
		{"8.1", "SELECT CHANGES()", {1}},
		{"8.2", "SELECT TOTAL_CHANGES()", {12}},
		{"9.0", "SELECT COUNT(*) FROM T2", {3}},
		{"9.1", "UPDATE T2 SET A = A + 3", {}},
        -- Inserts to an ephemeral space are not counted.
		{"9.2", "SELECT CHANGES()", {3}},
		{"9.3", "SELECT TOTAL_CHANGES()", {15}},
		{"11.0", "DELETE FROM T1", {}},
		{"11.1", "SELECT CHANGES()", {0}},
		{"11.2", "SELECT TOTAL_CHANGES()", {15}},
		{"12.0", "DELETE FROM T2 WHERE A < 100", {}},
		{"12.1", "SELECT CHANGES()", {3}},
		{"12.2", "SELECT TOTAL_CHANGES()", {18}},
        -- Transactions/savepoints.
		{"13.0", "START TRANSACTION", {}},
		{"13.1", "INSERT INTO T1 VALUES(11)", {}},
		{"13.2", "SELECT CHANGES()", {1}},
		{"13.3", "SELECT TOTAL_CHANGES()", {19}},
		{"13.4", "SAVEPOINT S1", {}},
		{"13.5", "INSERT INTO T1 VALUES(12)", {}},
		{"13.6", "SELECT CHANGES()", {1}},
		{"13.7", "SELECT TOTAL_CHANGES()", {20}},
		{"13.8", "ROLLBACK TO SAVEPOINT S1", {}},
		{"13.9", "SELECT CHANGES()", {1}},
		{"13.10", "SELECT TOTAL_CHANGES()", {20}},
		{"13.11", "COMMIT", {}},
		{"13.12", "SELECT CHANGES()", {1}},
		{"13.13", "SELECT TOTAL_CHANGES()", {20}},

	})

test:finish_test()
