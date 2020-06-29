--  Question 1.1

CREATE TABLE cs598_question_1_1_local_2
    (origin STRING,
    dest STRING,
    flight_total BIGINT)
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\n';
    

CREATE EXTERNAL TABLE ddb_cs598_question_1_1_test
    (origin STRING,
    dest STRING,
    flight_total BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_1_1",
    "dynamodb.column.mapping"="origin:id, dest:dest_airport, flight_total:flight_total_agg"
);




--  Question 1.2

CREATE TABLE cs598_question_1_2_local
    (dest STRING,
    arrival_performance BIGINT)
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\n';
    

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