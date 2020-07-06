INSERT OVERWRITE TABLE 
ddb_cs598_question_1_1_local 
select Origin, Dest, origin_total + dest_total as total 
from ((select Origin, count(Origin) as origin_total from `cs598.cs598task1` Group by Origin) as a 
JOIN 
(select Dest, count(Dest) as dest_total from 
`cs598.cs598task1` Group by Dest) as b
ON a.Origin = b.Dest) order by total desc limit 10;