-- @suite
-- @setup
-- @label:bvt

-- @case
-- @desc: Test COERCIBILITY function with different string types
-- @label:bvt
SELECT COERCIBILITY('abc') AS result1;
SELECT COERCIBILITY('test string') AS result2;
SELECT COERCIBILITY('') AS result3;
SELECT COERCIBILITY('Hello World!') AS result4;

-- @case
-- @desc: Test COERCIBILITY function with NULL and numeric types
-- @label:bvt
SELECT COERCIBILITY(NULL) AS null_result;
SELECT COERCIBILITY(123) AS int_result;
SELECT COERCIBILITY(0123) AS octal_result;
SELECT COERCIBILITY('0123') AS string_0123;
SELECT COERCIBILITY(45.67) AS float_result;
SELECT COERCIBILITY(CAST(100 AS BIGINT)) AS bigint_result;
SELECT COERCIBILITY(CAST(200 AS DECIMAL(10,2))) AS decimal_result;

-- @case
-- @desc: Test COERCIBILITY function with unicode strings
-- @label:bvt
SELECT COERCIBILITY('你好') AS result5;
SELECT COERCIBILITY('こんにちは') AS result6;
SELECT COERCIBILITY('안녕하세요') AS result7;
SELECT COERCIBILITY('😀🎉') AS result8;

-- @case
-- @desc: Test COERCIBILITY function with column values
-- @label:bvt
CREATE TABLE test_coercibility (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    description TEXT,
    age INT,
    score DECIMAL(5,2)
);

INSERT INTO test_coercibility VALUES (1, 'Alice', 'First user', 25, 95.5);
INSERT INTO test_coercibility VALUES (2, 'Bob', 'Second user', 30, 88.0);
INSERT INTO test_coercibility VALUES (3, '李明', '第三个用户', NULL, NULL);

-- Test with column references
SELECT id, name, COERCIBILITY(name) AS name_coercibility FROM test_coercibility ORDER BY id;
SELECT id, description, COERCIBILITY(description) AS desc_coercibility FROM test_coercibility ORDER BY id;
SELECT id, age, COERCIBILITY(age) AS age_coercibility FROM test_coercibility ORDER BY id;
SELECT id, score, COERCIBILITY(score) AS score_coercibility FROM test_coercibility ORDER BY id;

-- @case
-- @desc: Test COERCIBILITY function with expressions
-- @label:bvt
SELECT COERCIBILITY(CONCAT('Hello', ' ', 'World')) AS concat_result;
SELECT COERCIBILITY(UPPER('test')) AS upper_result;
SELECT COERCIBILITY(LOWER('TEST')) AS lower_result;
SELECT COERCIBILITY(SUBSTRING('Hello World', 1, 5)) AS substring_result;

-- @case
-- @desc: Test COERCIBILITY function with explicit COLLATE (should return 0)
-- @label:bvt
SELECT COERCIBILITY('abc' COLLATE utf8mb4_bin) AS explicit_collate_0;
SELECT COERCIBILITY('test' COLLATE utf8mb4_general_ci) AS explicit_collate_1;

-- @case
-- @desc: Test COERCIBILITY function with system constants (should return 3)
-- @label:bvt
SELECT COERCIBILITY(USER()) AS user_result;
SELECT COERCIBILITY(VERSION()) AS version_result;
SELECT COERCIBILITY(DATABASE()) AS database_result;

-- @case
-- @desc: Test COERCIBILITY function with binary/no collation types (should return 1)
-- @label:bvt
SELECT COERCIBILITY(CAST('test' AS VARBINARY(100))) AS binary_result;
SELECT COERCIBILITY(BINARY 'test') AS binary_literal;

-- @case
-- @desc: Test COERCIBILITY function returning 0 (explicit COLLATE expressions)
-- @label:bvt
SELECT COERCIBILITY('hello' COLLATE utf8mb4_bin) AS collate_0_1;
SELECT COERCIBILITY('world' COLLATE utf8mb4_unicode_ci) AS collate_0_2;
SELECT COERCIBILITY('测试' COLLATE utf8mb4_bin) AS collate_0_3;
SELECT COERCIBILITY(CAST('test' AS CHAR(50)) COLLATE utf8mb4_general_ci) AS collate_0_4;

-- @case
-- @desc: Test COERCIBILITY function returning 2 (column references - implicit collation)
-- @label:bvt
CREATE TABLE test_coercibility_cols (
    col1 VARCHAR(50),
    col2 CHAR(100),
    col3 TEXT,
    col4 VARCHAR(200)
);

INSERT INTO test_coercibility_cols VALUES ('value1', 'char1', 'text1', 'varchar1');
INSERT INTO test_coercibility_cols VALUES ('value2', 'char2', 'text2', 'varchar2');
INSERT INTO test_coercibility_cols VALUES ('value3', 'char3', 'text3', 'varchar3');

SELECT COERCIBILITY(col1) AS col1_coercibility FROM test_coercibility_cols ORDER BY col1 LIMIT 1;
SELECT COERCIBILITY(col2) AS col2_coercibility FROM test_coercibility_cols ORDER BY col2 LIMIT 1;
SELECT COERCIBILITY(col3) AS col3_coercibility FROM test_coercibility_cols ORDER BY col3 LIMIT 1;
SELECT COERCIBILITY(col4) AS col4_coercibility FROM test_coercibility_cols ORDER BY col4 LIMIT 1;

-- Test with multiple columns in one query
SELECT col1, COERCIBILITY(col1) AS c1, col2, COERCIBILITY(col2) AS c2 FROM test_coercibility_cols ORDER BY col1 LIMIT 1;

DROP TABLE IF EXISTS test_coercibility_cols;

-- @case
-- @desc: Test COERCIBILITY function returning 5 (NULL values - ignorable)
-- @label:bvt
SELECT COERCIBILITY(NULL) AS null_5_1;
SELECT COERCIBILITY(CAST(NULL AS VARCHAR(50))) AS null_5_2;
SELECT COERCIBILITY(CAST(NULL AS INT)) AS null_5_3;
SELECT COERCIBILITY(CAST(NULL AS DECIMAL(10,2))) AS null_5_4;

-- Test NULL in column context
CREATE TABLE test_coercibility_null (
    id INT,
    name VARCHAR(50),
    value INT
);

INSERT INTO test_coercibility_null VALUES (1, NULL, NULL);
INSERT INTO test_coercibility_null VALUES (2, 'test', 100);
INSERT INTO test_coercibility_null VALUES (3, NULL, 200);

SELECT id, COERCIBILITY(name) AS name_coercibility FROM test_coercibility_null WHERE name IS NULL ORDER BY id;
SELECT id, COERCIBILITY(value) AS value_coercibility FROM test_coercibility_null WHERE value IS NULL ORDER BY id;

DROP TABLE IF EXISTS test_coercibility_null;

-- @case
-- @desc: Cleanup
-- @label:bvt
DROP TABLE IF EXISTS test_coercibility;

