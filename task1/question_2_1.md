# Group 2


## Question 1
```
# Creating external DB in dynamoDB:

CREATE EXTERNAL TABLE ddb_cs598_question_2_1_2
    (airport_code STRING)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "q_2_1-2_ref",
    "dynamodb.column.mapping"="airport_code:airport_code"
);

#insert into dynamodb

INSERT OVERWRITE TABLE ddb_cs598_question_1_2 select Dest, sum(ArrDelay) as arrival-performance  from `cs598.cs598task1` having arrival_performance is not null  GROUP BY Dest SORT BY arrival-performance asc  limit 10;

select Dest, sum(ArrDelay) as arrival_performance  from `cs598.cs598task1`  GROUP BY Dest having arrival_performance is not null SORT BY arrival_performance asc  limit 10;

INSERT OVERWRITE TABLE ddb_cs598_question_2_1  select origin, carrier, avg(depdelay) as avg_depdelay from cs598.cs598task1 where origin IN (select * from ddb_cs598_question_2_1_2 limit 1) group by origin, carrier having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

select origin, carrier, avg(depdelay) as avg_depdelay
from cs598.cs598task1 where origin IN (select * from ddb_cs598_question_2_1_2 limit 1) group by  
origin, carrier having avg_depdelay is not null
sort by avg_depdelay asc limit 10;

```
Results:
```
CMI	OH	0.6116264687693259
CMI	US	2.033047346679377
CMI	TW	4.735353535353536
CMI	PI	5.587096774193548
CMI	DH	6.027888446215139
CMI	EV	6.665137614678899
CMI	MQ	8.016004886988393


BWI	F9	0.7562437562437563
BWI	PA	4.761904761904762
BWI	CO	5.1485301783373085
BWI	YV	5.496503496503497
BWI	NW	5.791685678899681
BWI	AA	6.103507291505722
BWI	9E	7.239805825242718
BWI	US	7.508907895286403
BWI	UA	7.734484766155638
BWI	FL	7.747100147034798

origin,carrier,avg_depdelay
MIA	9E	-3.0
MIA	EV	1.2026431718061674
MIA	RU	1.3021664766248575
MIA	TZ	1.782243551289742
MIA	XE	2.745644599303136
MIA	PA	3.7389675499972452
MIA	NW	4.5040642076502735
MIA	US	6.061599120445843
MIA	UA	6.88091441401217
MIA	ML	7.504550050556118

LAX	RU	1.9483870967741936
LAX	MQ	2.407221858260434
LAX	OO	4.2219592877139975
LAX	FL	4.725127379994636
LAX	TZ	4.763940985246312
LAX	NW	4.951674289186682
LAX	F9	5.729155372438469
LAX	HA	5.813645621181263
LAX	YV	6.024156085475379
LAX	EA	6.4884189325276935

IAH	NW	3.51326380228999
IAH	PA	3.9847272727272727
IAH	RU	4.798695924258635
IAH	US	5.042346298619824
IAH	F9	5.545243619489559
IAH	PI	5.745321399511798
IAH	AA	5.785937409374508
IAH	WN	6.449623532414497
IAH	TW	6.457104557640751
IAH	OO	6.58795822240426


SFO	TZ	3.952415634862831
SFO	MQ	4.853923777799549
SFO	F9	5.162444663059518
SFO	PA	5.305942773294204
SFO	NW	5.590084712688538
SFO	DL	6.570832797840895
SFO	CO	6.837172913980928
SFO	US	7.137510103662098
SFO	AA	8.02075832871468
SFO	TW	8.069147860259486


{
  "origin": "CMI",
  "carrier": "US",
  "avg_depdelay": 2.033047346679377
}

```

Sample solution:
```
CMI
(OH, 0.61)
(US, 2.03)
(TW, 4.12)
(PI, 4.46)
(DH, 6.03)
(EV, 6.67)
(MQ, 8.02)




```