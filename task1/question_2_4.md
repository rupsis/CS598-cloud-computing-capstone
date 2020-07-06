# Group 2


## Question 3
```
# Creating external DB in dynamoDB:

CREATE EXTERNAL TABLE ddb_cs598_question_2_3_4_ref
    (origin_airport STRING,
    dest_airport STRING)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "q_2_3-4_ref",
    "dynamodb.column.mapping"="origin_airport:origin_airport,dest_airport:dest_airport"
);

#insert into dynamodb

select origin, dest, (sum(arrdelay) / count(arrdelay)) as mean_arrdelay 
from cs598.cs598task1 where origin == 'CMI' and Dest == 'ORD' 
group by origin, dest
sort by mean_arrdelay asc limit 10;

select origin, dest, (sum(arrdelay) / count(arrdelay)) as mean_arrdelay 
from cs598.cs598task1 where origin == 'IND' and Dest == 'CMH' 
group by origin, dest
sort by mean_arrdelay asc limit 10;

select origin, dest, (sum(arrdelay) / count(arrdelay)) as mean_arrdelay 
from cs598.cs598task1 where origin == 'DFW' and Dest == 'IAH' 
group by origin, dest
sort by mean_arrdelay asc limit 10;

select origin, dest, (sum(arrdelay) / count(arrdelay)) as mean_arrdelay 
from cs598.cs598task1 where origin == 'LAX' and Dest == 'SFO' 
group by origin, dest
sort by mean_arrdelay asc limit 10;

select origin, dest, (sum(arrdelay) / count(arrdelay)) as mean_arrdelay 
from cs598.cs598task1 where origin == 'JFK' and Dest == 'LAX' 
group by origin, dest
sort by mean_arrdelay asc limit 10;

select origin, dest, (sum(arrdelay) / count(arrdelay)) as mean_arrdelay 
from cs598.cs598task1 where origin == 'ATL' and Dest == 'PHX' 
group by origin, dest
sort by mean_arrdelay asc limit 10;

{
  "origin": "ATL",
  "dest": "PHX",
  "mean_arrdelay": 9.063504653130288
}

```
Results:
```
origin, Dest, Carrier, Average Arrival Delay



CMI	ORD	10.14366290643663

IND	CMH	2.038634011728182

DFW	IAH	7.777138915885507

LAX	SFO	10.014323186426155

JFK	LAX	6.734613611479283

ATL	PHX	9.063504653130288




```

Sample solution:
```
X → Y: (Airline, Average delay in minutes)

X → Y: Average delay in minutes

CMI → ORD: 10.14
IND → CMH: 2.90
DFW → IAH: 7.65
LAX → SFO: 9.59
JFK → LAX: 6.64
ATL → PHX: 9.02



```

