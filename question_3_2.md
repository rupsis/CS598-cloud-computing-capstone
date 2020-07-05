# Group 3


## Question 2
```
# Creating external DB in dynamoDB:

CREATE EXTERNAL TABLE ddb_cs598_question_3_2
    (origin STRING, 
    dest_1 STRING, 
    carrier_1 STRING,
    fightNum_1 STRING, 
    arrDelay_1 BIGINT, 
    depTime_1 BIGINT, 
    fightDate_1 STRING,
    origin_2 STRING,
    dest_2 STRING,
    carrier_2 STRING,
    fightNum_2 STRING,
    arrDelay_2 BIGINT,
    depTime_2 BIGINT,
    fightDate_2 STRING,
    total_delay BITING
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_3_2",
    "dynamodb.column.mapping"="origin:origin,dest_1:dest_1,carrier_1:carrier_1,fightNum_1:fightNum_1,arrDelay_1:arrDelay_1,depTime_1:depTime_1,fightDate_1:fightDate_1,origin_2:origin_2,dest_2:dest_2,carrier_2:carrier_2,fightNum_2:fightNum_2,arrDelay_2:arrDelay_2,depTime_2:depTime_2,fightDate_2:fightDate_2,total_delay:total_delay"
);

#insert into dynamodb


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
    Dest = 'SFO' and 
    Year = 2008 and
    Month = 7 and 
    DayofMonth = 12 and 
    CRSDepTime < 1200 and
    ArrDelay is not null
    sort by ArrDelay asc limit 1) as a 
JOIN 
    (select origin, dest, Carrier, FlightNum, ArrDelay, CRSDepTime, flightdate
        from cs598.cs598task1 where 
        origin = 'SFO' and
        Dest = 'PHX' and 
        Year = 2008 and
        Month = 7 and 
        DayofMonth = 14 and 
        CRSDepTime > 1200 and
        ArrDelay is not null
        sort by ArrDelay asc limit 1) as b 
 ON (a.dest = b.origin);

[
,
,
,
,

]





```
Results:
```

CMI → ORD → LAX, 04/03/2008:

origin, dest_1, carrier_1, fightNum_1, arrDelay_1, depTime_1, fightDate_1, origin_2, dest_2, carrier_2, fightNum_2, arrDelay_2, depTime_2, fightDate_2, total_delay

CMI	ORD	MQ	4278	-14.0	710	2008-03-04	ORD	LAX	AA	607	-24.0	1950	2008-03-06	-38.0

JAX → DFW → CRP, 09/09/2008:
JAX	DFW	AA	845	1.0	725	2008-09-09	DFW	CRP	MQ	3627	-7.0	1645	2008-09-11	-6.0

SLC -> BFL -> LAX, 01/04/2008: 
SLC	BFL	OO	3755	12.0	1100	2008-04-01	BFL	LAX	OO	5429	6.0	1455	2008-04-03	18.0

LAX -> SFO -> PHX, 12/07/2008:
LAX SFO WN  3534  -13.0  650   2008-7-12   SFO  PHX  US  412    -19.0   1925 2008-07-14 -32.0


DFW -> ORD -> DFW, 10/06/2008:
DFW	ORD	UA	1104	-21.0	700	2008-06-10	ORD	DFW	AA	2341	-10.0	1645	2008-06-12	-31.0


LAX -> ORD -> JFK, 1/1/2008:
LAX	ORD	UA	944	1.0	705	2008-01-01	ORD	JFK	B6	918	-7.0	1900	2008-01-03	-6.0


Total arrival delay: -32.0
First leg:
Origin: LAX
Destination: SFO
Airline/Flight Number: WN 3534
Sched Depart: 6:50 12/07/2008
Arrival delay: -13.0
2. Second leg:

Origin: SFO
Destination: PHX
Airline/Flight Number: US 412
Sched Depart: 19:25 14/07/2008
Arrival delay: -19.0

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