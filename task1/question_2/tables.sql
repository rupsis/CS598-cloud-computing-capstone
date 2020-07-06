--  Question 2.1

CREATE EXTERNAL TABLE ddb_cs598_question_2_1_batch
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


CREATE EXTERNAL TABLE cs598_question_2_4_batch
    (origin STRING,
    dest STRING,
    mean_arrdelay BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_2_4",
    "dynamodb.column.mapping"="day_of_the_week:days_of_the_week,arrival_performance:arrival_performance"
);