--  Question 1.1

INSERT OVERWRITE TABLE cs598_question_1_1_local 
select Origin, Dest, origin_total + dest_total as total from ((select Origin, count(Origin) as origin_total from `cs598.cs598task1` Group by Origin) as a 
JOIN 
(select Dest, count(Dest) as dest_total from 
`cs598.cs598task1` Group by Dest) as b
ON a.Origin = b.Dest) order by total desc limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_1_1 
SELECT *  FROM cs598_question_1_1_local;

INSERT OVERWRITE TABLE ddb_cs598_question_1_1 
SELECT origin, dest, flight_total  FROM cs598_question_1_1_local;

INSERT INTO TABLE ddb_cs598_question_1_1 origin, dest, flight_total  VALUES ('test', 'test', 1);


--  Question 1.2

INSERT OVERWRITE TABLE ddb_cs598_question_1_2_01
select Carrier, avg(ArrDelay) as arrival_performance  
from `cs598.cs598task1`  GROUP BY Carrier having 
arrival_performance is not null SORT BY arrival_performance asc  limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_1_2 
SELECT *  FROM cs598_question_1_2_local;


-- Question 1.3

INSERT OVERWRITE TABLE ddb_cs598_question_1_3_01 
select DayOfWeek, avg(ArrDelay) as arrival_performance  
from `cs598.cs598task1`  GROUP BY DayOfWeek having 
arrival_performance is not null SORT BY arrival_performance asc  limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_1_3 
SELECT *  FROM cs598_question_1_3_local;