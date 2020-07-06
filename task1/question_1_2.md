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

select Carrier, avg(ArrDelay) as arrival_performance  from `cs598.cs598task1`  GROUP BY Carrier having arrival_performance is not null SORT BY arrival_performance asc  limit 10;


```
Results:
```
# based on sum(ArrDelay)
HA	-1.01180434574519
AQ	1.1569234424812056
PA	4.265420017873101
ML	4.747609195734892
NW	5.45229911101284
F9	5.465881148819851
WN	5.602039015611727
OO	5.736312463662878
9E	5.8671846616957595
EA	6.060172358025279
```

HA: -1.01
AQ: 1.16
PS: 1.45
ML (1): 4.75
PA (1): 5.32
F9: 5.47
NW: 5.56
WN: 5.56
OO: 5.74
9E: 5.87