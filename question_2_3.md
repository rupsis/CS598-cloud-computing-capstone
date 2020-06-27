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

INSERT OVERWRITE TABLE ddb_cs598_question_1_2 select Dest, sum(ArrDelay) as arrival-performance  from `cs598.cs598task1` having arrival_performance is not null  GROUP BY Dest SORT BY arrival-performance asc  limit 10;

select Dest, sum(ArrDelay) as arrival_performance  from `cs598.cs598task1`  GROUP BY Dest having arrival_performance is not null SORT BY arrival_performance asc  limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_2_1  select origin, carrier, avg(depdelay) as avg_depdelay from cs598.cs598task1 where origin IN (select * from ddb_cs598_question_2_1_2 limit 1) group by origin, carrier having avg_depdelay is not null
sort by avg_depdelay asc limit 10;


select origin, dest, carrier, avg(arrdelay) as avg_arrdelay
from cs598.cs598task1 where origin IN (select origin_airport from ddb_cs598_question_2_3_4_ref limit 1 offset 2) and Dest IN (select dest_airport from ddb_cs598_question_2_3_4_ref limit 1 offset 2)  group by  
origin, dest, carrier having avg_arrdelay is not null
sort by avg_arrdelay asc limit 10;


select origin, dest, carrier, avg(arrdelay) as avg_arrdelay
from cs598.cs598task1 where origin == 'ATL' and Dest == 'PHX' group by  
origin, dest, carrier having avg_arrdelay is not null
sort by avg_arrdelay asc limit 10;



```
Results:
```
origin, Dest, Carrier, Average Arrival Delay

CMI	ORD	MQ	10.14366290643663

IND	CMH	CO	-2.6579673776662482
IND	CMH	NW	3.941798941798942
IND	CMH	AA	5.5
IND	CMH	HP	5.697254901960784
IND	CMH	US	6.237020316027088
IND	CMH	DL	10.6875

DFW	IAH	PA	-1.5964912280701755
DFW	IAH	EV	5.0925133689839575
DFW	IAH	CO	6.543978685024906
DFW	IAH	OO	7.564007421150278
DFW	IAH	RU	7.7914915966386555
DFW	IAH	XE	8.442865779927448
DFW	IAH	AA	8.545200789587318
DFW	IAH	DL	8.904884655714785
DFW	IAH	MQ	9.103211009174313

LAX	SFO	TZ	-7.619047619047619
LAX	SFO	F9	-2.028685790527018
LAX	SFO	EV	6.964630225080386
LAX	SFO	MQ	7.8077634011090575
LAX	SFO	AA	8.181292220175184
LAX	SFO	WN	8.79205149734117
LAX	SFO	US	8.822730864755023
LAX	SFO	CO	9.659957627118644
LAX	SFO	UA	10.364480979470095
LAX	SFO	PA	10.379324462640737

JFK	LAX	UA	3.329564447940222
JFK	LAX	HP	6.680599369085174
JFK	LAX	AA	7.214716627006736
JFK	LAX	DL	7.934460351304701
JFK	LAX	PA	9.1992700729927
JFK	LAX	TW	12.125

ATL	PHX	FL	4.552631578947368
ATL	PHX	US	6.28811524609844
ATL	PHX	HP	8.481436314363144
ATL	PHX	EA	8.662650602409638
ATL	PHX	DL	9.900493798805696





```

Sample solution:
```
X → Y: (Airline, Average delay in minutes)

CMI → ORD
(MQ, 10.14)

IND → CMH
(CO, -2.55)
(AA, 5.5)
(HP, 5.70)
(NW, 5.76)
(US, 6.88)
(DL, 10.69)
(EA, 10.81)

DFW → IAH
PA (1), -1.60)
(EV, 5.09)
(UA, 5.41)
(CO, 6.49)
(OO, 7.56)
(XE, 8.09)
(AA, 8.38)
(DL, 8.60)
(MQ, 9.10)

LAX → SFO
(TZ, -7.62)
(PS, -2.15)
(F9, -2.03)
(EV, 6.96)
(AA, 7.39)
(MQ, 7.81)
(US, 7.96)
(WN, 8.79)
(CO, 9.35)
(NW, 9.85)




```

