# Group 3


## Question 2
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


# for each airport code (CMI, BWI, MIA, LAX, IAH, SFO) run the folowing query



select origin, dest, Carrier, FlightNum, ArrDelay, flightdate
from cs598.cs598task1 where 
origin = 'JAX' and
Dest = 'DFW' and 
Year = 2008 and
Month = 9 and 
DayofMonth = 9 and 
CRSDepTime < 1200
sort by ArrDelay asc limit 3;

select origin, dest, Carrier, FlightNum, ArrDelay, flightdate
from cs598.cs598task1 where 
origin = 'ORD' and
Dest = 'LAX' and 
Year = 2008 and
Month = 3 and 
DayofMonth = 6 and 
CRSDepTime > 1200
sort by ArrDelay asc limit 3;


```
Results:
```

CMI -> ORD:
CMI	ORD	MQ	4278	-14.0
CMI	ORD	MQ	4401	-11.0
CMI	ORD	MQ	4374	-1.0
CMI	ORD	MQ	4373	11.0

ORD -> LAX
ORD	LAX	AA	607	-24.0
ORD	LAX	UA	945	-23.0
ORD	LAX	AA	1345	-21.0
ORD	LAX	UA	943	-19.0
ORD	LAX	AA	1407	-17.0
ORD	LAX	AA	557	-12.0
ORD	LAX	UA	127	-12.0
ORD	LAX	UA	121	-4.0
ORD	LAX	AA	455	1.0
ORD	LAX	UA	129	2.0




```

Sample solution:
```
Delays are in minutes, date is dd/MM/yyyy

CMI → ORD → LAX, 04/03/2008:

Total arrival delay: -38.0
First leg:
Origin: CMI
Destination: ORD
Airline/Flight Number: MQ 4278
Sched Depart: 7:10 04/03/2008
Arrival delay: -14.0

2. Second leg:

Origin: ORD
Destination: LAX
Airline/Flight Number: AA 607
Sched Depart: 19:50 06/03/2008
Arrival delay: -24.0



JAX → DFW → CRP, 09/09/2008:

Total arrival delay: -6.0
First leg:
Origin: JAX
Destination: DFW
Airline/Flight Number: AA 845
Sched Depart: 7:25 09/09/2008
Arrival delay: 1.0
2. Second leg:

Origin: DFW
Destination: CRP
Airline/Flight Number: MQ 3627
Sched Depart: 16:45 11/09/2008
Arrival delay: -7.0


```