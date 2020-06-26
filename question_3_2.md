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
sort by ArrDelay asc limit 10;

select origin, dest, Carrier, FlightNum, ArrDelay, flightdate
from cs598.cs598task1 where 
origin = 'DFW' and
Dest = 'CPR' and 
Year = 2008 and
Month = 9 and 
DayofMonth = 11 and 
CRSDepTime > 1200
sort by ArrDelay asc limit 3;

select *, (a.ArrDelay + b.ArrDelay) as total_delay  from 
    (select origin, dest, Carrier, FlightNum, ArrDelay, CRSDepTime, flightdate
    from cs598.cs598task1 where 
    origin = 'SLC' and
    Dest = 'BFL' and 
    Year = 2008 and
    Month = 4 and 
    DayofMonth = 1 and 
    CRSDepTime < 1200 and
    ArrDelay is not null
    sort by ArrDelay asc limit 1) as a 
JOIN 
    (select origin, dest, Carrier, FlightNum, ArrDelay, CRSDepTime, flightdate
        from cs598.cs598task1 where 
        origin = 'BFL' and
        Dest = 'LAX' and 
        Year = 2008 and
        Month = 4 and 
        DayofMonth = 3 and 
        CRSDepTime > 1200 and
        ArrDelay is not null
        sort by ArrDelay asc) as b 
 ON (a.dest = b.origin) order by total_delay asc limit 5;



select *, (a.ArrDelay + b.ArrDelay) as total_delay  from 
    (select origin, dest, Carrier, FlightNum, ArrDelay, CRSDepTime, flightdate
    from cs598.cs598task1 where 
    origin = 'LAX' and
    Dest = 'ORD' and 
    Year = 2008 and
    Month = 1 and 
    DayofMonth = 1 and 
    CRSDepTime < 1200 and
    ArrDelay is not null
    sort by ArrDelay asc limit 1) as a 
JOIN 
    (select origin, dest, Carrier, FlightNum, ArrDelay, CRSDepTime, flightdate
        from cs598.cs598task1 where 
        origin = 'ORD' and
        Dest = 'JFK' and 
        Year = 2008 and
        Month = 1 and 
        DayofMonth = 3 and 
        CRSDepTime > 1200 and
        ArrDelay is not null
        sort by ArrDelay asc limit 1) as b 
 ON (a.dest = b.origin);





```
Results:
```

CMI → ORD → LAX, 04/03/2008:

CMI	ORD	MQ	4278	-14.0	710	2008-03-04	ORD	LAX	AA	607	-24.0	1950	2008-03-06	-38.0

JAX → DFW → CRP, 09/09/2008:
JAX	DFW	AA	845	1.0	725	2008-09-09	DFW	CRP	MQ	3627	-7.0	1645	2008-09-11	-6.0

SLC -> BFL -> LAX, 01/04/2008: 
SLC	BFL	OO	3755	12.0	1100	2008-04-01	BFL	LAX	OO	5429	6.0	1455	2008-04-03	18.0

LAX -> SFO -> PHX, 12/07/2008:
LAX	SFO	WN	3534	-13.0	650	2008-07-12	SFO	PHX	US	408	-9.0	1715	2008-07-15	-22.0

DFW -> ORD -> DFW, 10/06/2008:
DFW	ORD	UA	1104	-21.0	700	2008-06-10	ORD	DFW	AA	2341	-10.0	1645	2008-06-12	-31.


LAX -> ORD -> JFK, 1/1/2008:
LAX	ORD	UA	944	1.0	705	2008-01-01	ORD	JFK	B6	918	-7.0	1900	2008-01-03	-6.0

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