--  Question 2.1

CREATE TABLE cs598_question_2_1_local
    (origin STRING,
    carrier STRING,
    avg_depdelay BIGINT);

CREATE EXTERNAL TABLE ddb_cs598_question_2_1
    (origin STRING,
    carrier STRING,
    avg_depdelay BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_2_1",
    "dynamodb.column.mapping"="origin:origin,carrier:carrier, avg_depdelay:avg_depdelay"
);


--  Question 2.2

CREATE TABLE cs598_question_2_2_local
    (origin STRING,
    dest STRING,
    avg_depdelay BIGINT);

CREATE EXTERNAL TABLE ddb_cs598_question_2_2
    (origin STRING,
    dest STRING,
    avg_depdelay BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_2_2",
    "dynamodb.column.mapping"="origin:origin,dest:dest,avg_depdelay:avg_depdelay"
);


--  Question 2.3
CREATE TABLE cs598_question_2_3_local
    (origin STRING,
    dest STRING,
    carrier STRING,
    avg_arrdelay BIGINT);

CREATE EXTERNAL TABLE ddb_cs598_question_2_3
    (origin STRING,
    dest STRING,
    carrier STRING,
    avg_arrdelay BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_2_3",
    "dynamodb.column.mapping"="origin:origin,dest:dest,carrier:carrier,avg_arrdelay:avg_arrdelay"
);

--  Question 2.4
CREATE TABLE cs598_question_1_3_local
    (day_of_the_week STRING,
    arrival_performance BIGINT);

CREATE EXTERNAL TABLE ddb_cs598_question_1_3
    (day_of_the_week STRING,
    arrival_performance BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_1_3",
    "dynamodb.column.mapping"="day_of_the_week:days_of_the_week,arrival_performance:arrival_performance"
);