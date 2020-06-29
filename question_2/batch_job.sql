--  Question 2.1
-- CMI, BWI, MIA, LAX, IAH, SFO, 

INSERT OVERWRITE TABLE cs598_question_2_1_local 
select origin, carrier, avg(depdelay) as avg_depdelay
from cs598.cs598task1 where origin == 'CMI' group by  
origin, carrier having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_1_local 
select origin, carrier, avg(depdelay) as avg_depdelay
from cs598.cs598task1 where origin == 'BWI' group by  
origin, carrier having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_1_local 
select origin, carrier, avg(depdelay) as avg_depdelay
from cs598.cs598task1 where origin == 'MIA' group by  
origin, carrier having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_1_local 
select origin, carrier, avg(depdelay) as avg_depdelay
from cs598.cs598task1 where origin == 'LAX' group by  
origin, carrier having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_1_local 
select origin, carrier, avg(depdelay) as avg_depdelay
from cs598.cs598task1 where origin == 'IAH' group by  
origin, carrier having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE cs598_question_2_1_local 
select origin, carrier, avg(depdelay) as avg_depdelay
from cs598.cs598task1 where origin == 'SFO' group by  
origin, carrier having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_2_1 
SELECT *  FROM cs598_question_2_1_local;


--  Question 2.2

INSERT OVERWRITE TABLE cs598_question_2_2_local
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

INSERT OVERWRITE TABLE cs598_question_1_3_local 
select DayOfWeek, avg(ArrDelay) as arrival_performance  
from `cs598.cs598task1`  GROUP BY DayOfWeek having 
arrival_performance is not null SORT BY arrival_performance asc  limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_1_3 
SELECT *  FROM cs598_question_1_3_local;

-- Question 2.4

INSERT OVERWRITE TABLE cs598_question_1_3_local 
select DayOfWeek, avg(ArrDelay) as arrival_performance  
from `cs598.cs598task1`  GROUP BY DayOfWeek having 
arrival_performance is not null SORT BY arrival_performance asc  limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_1_3 
SELECT *  FROM cs598_question_1_3_local;