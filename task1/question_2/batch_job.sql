--  Question 2.1

--  To generate all permutations 

CREATE EXTERNAL TABLE ddb_cs598_question_2_1_batch
    (origin STRING,
    carrier STRING,
    avg_depdelay BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_2_1",
    "dynamodb.column.mapping"="origin:origin,carrier:carrier, avg_depdelay:avg_depdelay"
);


INSERT OVERWRITE TABLE  ddb_cs598_question_2_1_batch
select origin, dest, avg(depdelay) as avg_depdelay 
from cs598.cs598task1 where origin == 'CMI' group by origin, dest having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE  ddb_cs598_question_2_1_batch
select origin, carrier, avg(depdelay) as avg_depdelay
from cs598.cs598task1 
where origin  = 'BWI'
 group by  origin, carrier having ddb_cs598_question_2_1_batch is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE  ddb_cs598_question_2_1_batch
select origin, carrier, avg(depdelay) as avg_depdelay
from cs598.cs598task1 
where origin  = 'MIA'
 group by  origin, carrier having ddb_cs598_question_2_1_batch is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE  ddb_cs598_question_2_1_batch
select origin, carrier, avg(depdelay) as avg_depdelay
from cs598.cs598task1 
where origin  = 'LAX'
 group by  origin, carrier having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE  ddb_cs598_question_2_1_batch
select origin, carrier, avg(depdelay) as avg_depdelay
from cs598.cs598task1 
where origin  = 'IAH'
 group by  origin, carrier having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE  ddb_cs598_question_2_1_batch
select origin, carrier, avg(depdelay) as avg_depdelay
from cs598.cs598task1 
where origin  = 'SFO'
 group by  origin, carrier having avg_depdelay is not null
sort by avg_depdelay asc limit 10;



--  Question 2.2

CREATE EXTERNAL TABLE cs598_question_2_2_batch
    (origin STRING,
    dest STRING,
    avg_depdelay BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_2_2",
    "dynamodb.column.mapping"="origin:origin,dest:dest,avg_depdelay:avg_depdelay"
);

INSERT OVERWRITE TABLE cs598_question_2_2_batch
select origin, dest, avg(depdelay) as avg_depdelay 
from cs598.cs598task1 where origin == 'CMI' group by origin, dest having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_2_local
select origin, dest, avg(depdelay) as avg_depdelay 
from cs598.cs598task1 where origin == 'BWI' group by origin, dest having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_2_local
select origin, dest, avg(depdelay) as avg_depdelay 
from cs598.cs598task1 where origin == 'MIA' group by origin, dest having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_2_local
select origin, dest, avg(depdelay) as avg_depdelay 
from cs598.cs598task1 where origin == 'LAX' group by origin, dest having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_2_local
select origin, dest, avg(depdelay) as avg_depdelay 
from cs598.cs598task1 where origin == 'IAH' group by origin, dest having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_2_local
select origin, dest, avg(depdelay) as avg_depdelay 
from cs598.cs598task1 where origin == 'SFO' group by origin, dest having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_2_2 
SELECT *  FROM cs598_question_2_2_local;


-- Question 2.3

CREATE EXTERNAL TABLE ddb_cs598_question_2_3_batch
    (origin STRING,
    dest STRING,
    carrier STRING,
    avg_arrdelay BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_2_3",
    "dynamodb.column.mapping"="origin:origin,dest:dest,carrier:carrier,avg_arrdelay:avg_arrdelay"
);

INSERT OVERWRITE TABLE ddb_cs598_question_2_3_batch 
select origin, dest, carrier, avg(arrdelay) as avg_arrdelay
from cs598.cs598task1 where origin == 'CMI' and Dest == 'ORD' group by  
origin, dest, carrier having avg_arrdelay is not null
sort by avg_arrdelay asc limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_2_3_batch 
select origin, dest, carrier, avg(arrdelay) as avg_arrdelay
from cs598.cs598task1 where origin == 'IND' and Dest == 'CMH' group by  
origin, dest, carrier having avg_arrdelay is not null
sort by avg_arrdelay asc limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_2_3_batch 
select origin, dest, carrier, avg(arrdelay) as avg_arrdelay
from cs598.cs598task1 where origin == 'DFW' and Dest == 'IAH' group by  
origin, dest, carrier having avg_arrdelay is not null
sort by avg_arrdelay asc limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_2_3_batch 
select origin, dest, carrier, avg(arrdelay) as avg_arrdelay
from cs598.cs598task1 where origin == 'LAX' and Dest == 'SFO' group by  
origin, dest, carrier having avg_arrdelay is not null
sort by avg_arrdelay asc limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_2_3_batch 
select origin, dest, carrier, avg(arrdelay) as avg_arrdelay
from cs598.cs598task1 where origin == 'JFK' and Dest == 'LAX' group by  
origin, dest, carrier having avg_arrdelay is not null
sort by avg_arrdelay asc limit 10;


INSERT OVERWRITE TABLE ddb_cs598_question_2_3_batch 
select origin, dest, carrier, avg(arrdelay) as avg_arrdelay
from cs598.cs598task1 where origin == 'ATL' and Dest == 'PHX' group by  
origin, dest, carrier having avg_arrdelay is not null
sort by avg_arrdelay asc limit 10;



-- Question 2.4

CREATE EXTERNAL TABLE cs598_question_2_4_batch
    (origin STRING,
    dest STRING,
    mean_arrdelay BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_2_4",
    "dynamodb.column.mapping"="day_of_the_week:days_of_the_week,arrival_performance:arrival_performance"
);

INSERT OVERWRITE TABLE cs598_question_2_4_batch 
select origin, dest, (sum(arrdelay) / count(arrdelay)) as mean_arrdelay 
from cs598.cs598task1 where origin == 'CMI' and Dest == 'ORD' 
group by origin, dest
sort by mean_arrdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_4_batch 
select origin, dest, (sum(arrdelay) / count(arrdelay)) as mean_arrdelay 
from cs598.cs598task1 where origin == 'IND' and Dest == 'CMH' 
group by origin, dest
sort by mean_arrdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_4_batch 
select origin, dest, (sum(arrdelay) / count(arrdelay)) as mean_arrdelay 
from cs598.cs598task1 where origin == 'DFW' and Dest == 'IAH' 
group by origin, dest
sort by mean_arrdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_4_batch 
select origin, dest, (sum(arrdelay) / count(arrdelay)) as mean_arrdelay 
from cs598.cs598task1 where origin == 'LAX' and Dest == 'SFO' 
group by origin, dest
sort by mean_arrdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_4_batch 
select origin, dest, (sum(arrdelay) / count(arrdelay)) as mean_arrdelay 
from cs598.cs598task1 where origin == 'JFK' and Dest == 'LAX' 
group by origin, dest
sort by mean_arrdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_4_batch 
select origin, dest, (sum(arrdelay) / count(arrdelay)) as mean_arrdelay 
from cs598.cs598task1 where origin == 'ATL' and Dest == 'PHX' 
group by origin, dest
sort by mean_arrdelay asc limit 10;


-----------------------------------------------------------------


INSERT OVERWRITE TABLE cs598_question_2_4_batch 
select origin, dest, sum(arrdelay) / count(arrdelay) as mean_arrdelay 
from cs598.cs598task1 where origin == 'IND' and Dest == 'CMH' group by  
origin, dest, carrier having avg_arrdelay is not null;

INSERT OVERWRITE TABLE cs598_question_2_4_batch 
select origin, dest, sum(arrdelay) / count(arrdelay) as mean_arrdelay 
from cs598.cs598task1 where origin == 'DFW' and Dest == 'IAH' group by  
origin, dest, carrier having avg_arrdelay is not null;

INSERT OVERWRITE TABLE cs598_question_2_4_batch 
select origin, dest, sum(arrdelay) / count(arrdelay) as mean_arrdelay 
from cs598.cs598task1 where origin == 'LAX' and Dest == 'SFO' group by  
origin, dest, carrier having avg_arrdelay is not null;

INSERT OVERWRITE TABLE cs598_question_2_4_batch 
select origin, dest, sum(arrdelay) / count(arrdelay) as mean_arrdelay 
from cs598.cs598task1 where origin == 'JFK' and Dest == 'LAX' group by  
origin, dest, carrier having avg_arrdelay is not null;

INSERT OVERWRITE TABLE cs598_question_2_4_batch 
select origin, dest, sum(arrdelay) / count(arrdelay) as mean_arrdelay 
from cs598.cs598task1 where origin == 'ATL' and Dest == 'PHX' group by  
origin, dest, carrier having avg_arrdelay is not null;


INSERT OVERWRITE TABLE cs598_question_2_4_batch 
select DayOfWeek, avg(ArrDelay) as arrival_performance  
from `cs598.cs598task1`  GROUP BY DayOfWeek having 
arrival_performance is not null SORT BY arrival_performance asc  limit 10;

INSERT OVERWRITE TABLE cs598_question_2_4_batch 
SELECT *  FROM cs598_question_1_3_local;