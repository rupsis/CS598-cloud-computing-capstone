# Group 2


## Question 1
```
# Creating external DB in dynamoDB:

CREATE EXTERNAL TABLE ddb_cs598_question_2_1
    (origin STRING,
    carrier STRING,
    departure_performance
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_2_1",
    "dynamodb.column.mapping"="dest:dest,arrival_performance:arrival_performance"
);

#insert into dynamodb

INSERT OVERWRITE TABLE ddb_cs598_question_1_2 select Dest, sum(ArrDelay) as arrival-performance  from `cs598.cs598task1` having arrival_performance is not null  GROUP BY Dest SORT BY arrival-performance asc  limit 10;

select Dest, sum(ArrDelay) as arrival_performance  from `cs598.cs598task1`  GROUP BY Dest having arrival_performance is not null SORT BY arrival_performance asc  limit 10;

select origin, carrier, SUM(depdelay) as departure_delay 
from cs598task1_test where origin == 'CMI' group by origin, carrier having departure_delay is not null
sort by departure_delay desc;


```
Results:
```
# based on sum(ArrDelay)
ITO	-5248.0
MKK	-1316.0
IYK	-1183.0
PMD	-921.0
LNY	-865.0
LIH	-444.0
CBM	0.0
GLH	5.0
MKC	12.0
PIR	33.0


# based on sum(ArrivalDelayGroups)
DFW	-553308
MSP	-494638
DTW	-435922
CLT	-295626
IAH	-295167
CVG	-289127
MEM	-250229
SLC	-210349
STL	-195777
PHX	-172250
```