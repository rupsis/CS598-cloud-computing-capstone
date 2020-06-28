--  Question 1.1

CREATE TABLE cs598_question_1_1_local
    (origin STRING,
    dest BIGINT,
    flight_total BIGINT);

CREATE EXTERNAL TABLE ddb_cs598_question_1_1
    (origin STRING,
    dest BIGINT,
    flight_total BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_1_1",
    "dynamodb.column.mapping"="origin:origin,dest:destflight_total:flight_total"
);


--  Question 1.2

CREATE TABLE cs598_question_1_2_local
    (dest STRING,
    arrival_performance BIGINT);

CREATE EXTERNAL TABLE ddb_cs598_question_1_2
    (dest STRING,
    arrival_performance BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_1_2",
    "dynamodb.column.mapping"="dest:airport,arrival_performance:arrival_performance"
);


--  Question 1.3
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