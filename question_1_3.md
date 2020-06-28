# Group 1




## Question 1
```
# Creating external DB in dynamoDB:

CREATE EXTERNAL TABLE ddb_cs598_question_1_2
    (dest STRING,
    arrival_performance BIGINT
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_1_2",
    "dynamodb.column.mapping"="dest:dest,arrival_performance:arrival_performance"
);

#insert into dynamodb

INSERT OVERWRITE TABLE ddb_cs598_question_1_2 select Dest, sum(ArrDelay) as arrival-performance  from `cs598.cs598task1` having arrival_performance is not null  GROUP BY Dest SORT BY arrival-performance asc  limit 10;

select Dest, avg(ArrDelay) as arrival_performance  from `cs598.cs598task1`  GROUP BY Dest having arrival_performance is not null SORT BY arrival_performance asc  limit 10;

select DayOfWeek, avg(ArrDelay) as arrival_performance  from `cs598.cs598task1`  GROUP BY DayOfWeek having arrival_performance is not null SORT BY arrival_performance asc  limit 10;


```
Results:
```
6	4.313248565091031
2	5.956452361818581
7	6.702123949857592
1	6.712999486963483
3	7.195133394418562
4	9.137028699756533
5	9.81604680982988


```

Saturday: 4.30
Tuesday: 5.99
Sunday: 6.61
Monday: 6.72
Wednesday: 7.20
Thursday: 9.09
Friday: 9.72