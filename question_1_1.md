# Group 1




## Question 1
```
# Creating external DB in dynamoDB:

CREATE EXTERNAL TABLE ddb_cs598_question_1_1
    (origin STRING,
    origin_total BIGINT,
    dest_total BIGINT,
    flight_total BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_1_1",
    "dynamodb.column.mapping"="origin:origin,origin_total:origin_total,dest_total: dest_total, flight_total:flight_total"
);

#insert into dynamodb

INSERT OVERWRITE TABLE ddb_cs598_question_1_1 select Origin, count(Origin), count(Dest), count(Origin) + count(Dest) as total  from `cs598.cs598task1` GROUP BY Origin SORT BY total desc  limit 10;

select Origin, count(Origin), count(Dest), count(Origin) + count(Dest) as total  from `cs598task1-test.cs598task1_test` GROUP BY Origin SORT BY total desc  limit 10;
```
Results:
```
ORD     5933570 5933570 11867140
ATL     5514110 5514110 11028220
DFW     5166167 5166167 10332334
LAX     3693057 3693057 7386114
PHX     3168258 3168258 6336516
DEN     2968855 2968855 5937710
DTW     2701919 2701919 5403838
IAH     2652789 2652789 5305578
MSP     2499691 2499691 4999382
SFO     2457753 2457753 4915506
```
