CREATE TABLE ddb_cs598_question_1_1_local
    (origin_hive STRING,
    origin_total_hive BIGINT,
    dest_total_hive BIGINT,
    flight_total_hive BIGINT)

    INSERT OVERWRITE TABLE ddb_cs598_question_1_1 select 
    origin_hive,
    origin_total_hive,
    dest_total_hive,
    flight_total_hive
    from ddb_cs598_question_1_1_local;



CREATE EXTERNAL TABLE ddb_cs598_question_1_1
    (origin_hive STRING,
    origin_total_hive BIGINT,
    dest_total_hive BIGINT,
    flight_total_hive BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_1_1",
    "dynamodb.column.mapping"="origin_hive:origin,origin_total_hive:origin_total,dest_total_hive: dest_total, flight_total_hive:flight_total"
);


INSERT OVERWRITE TABLE ddb_cs598_question_1_1 select Origin, count(Origin), count(Dest), count(Origin) + count(Dest) as total  from `cs598.cs598task1` GROUP BY Origin SORT BY total desc  limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_1_1_local select Origin, count(Origin), count(Dest), count(Origin) + count(Dest) as total  from `cs598.cs598task1` GROUP BY Origin SORT BY total desc  limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_1_1_local select Origin, count(Origin), count(Dest), count(Origin) + count(Dest) as total  from `cs598task1-test.cs598task1_test` GROUP BY Origin SORT BY total desc  limit 10;


INSERT INTO TABLE ddb_cs598_question_1_1 VALUES ('test', 1, 1, 1);