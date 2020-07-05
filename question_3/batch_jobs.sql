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
    total_delay BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598_question_3_2",
    "dynamodb.column.mapping"="origin:origin,dest_1:dest_1,carrier_1:carrier_1,fightNum_1:fightNum_1,arrDelay_1:arrDelay_1,depTime_1:depTime_1,fightDate_1:fightDate_1,origin_2:origin_2,dest_2:dest_2,carrier_2:carrier_2,fightNum_2:fightNum_2,arrDelay_2:arrDelay_2,depTime_2:depTime_2,fightDate_2:fightDate_2,total_delay:total_delay"
);


INSERT OVERWRITE TABLE ddb_cs598_question_3_2
select *, (a.ArrDelay + b.ArrDelay) as total_delay  from 
    (select origin, dest, Carrier, FlightNum, ArrDelay, CRSDepTime, flightdate
    from cs598.cs598task1 where 
    origin = 'CMI' and
    Dest = 'ORD' and 
    Year = 2008 and
    Month = 3 and 
    DayofMonth = 4 and 
    CRSDepTime < 1200 and
    ArrDelay is not null
    sort by ArrDelay asc limit 1) as a 
JOIN 
    (select origin, dest, Carrier, FlightNum, ArrDelay, CRSDepTime, flightdate
        from cs598.cs598task1 where 
        origin = 'ORD' and
        Dest = 'LAX' and 
        Year = 2008 and
        Month = 3 and 
        DayofMonth = 6 and 
        CRSDepTime > 1200 and
        ArrDelay is not null
        sort by ArrDelay asc limit 1) as b 
 ON (a.dest = b.origin);


 INSERT OVERWRITE TABLE ddb_cs598_question_3_2
select *, (a.ArrDelay + b.ArrDelay) as total_delay  from 
    (select origin, dest, Carrier, FlightNum, ArrDelay, CRSDepTime, flightdate
    from cs598.cs598task1 where 
    origin = 'JAX' and
    Dest = 'DFW' and 
    Year = 2008 and
    Month = 9 and 
    DayofMonth = 9 and 
    CRSDepTime < 1200 and
    ArrDelay is not null
    sort by ArrDelay asc limit 1) as a 
JOIN 
    (select origin, dest, Carrier, FlightNum, ArrDelay, CRSDepTime, flightdate
        from cs598.cs598task1 where 
        origin = 'DFW' and
        Dest = 'CRP' and 
        Year = 2008 and
        Month = 9 and 
        DayofMonth = 11 and 
        CRSDepTime > 1200 and
        ArrDelay is not null
        sort by ArrDelay asc limit 1) as b 
 ON (a.dest = b.origin);

  INSERT OVERWRITE TABLE ddb_cs598_question_3_2
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
        sort by ArrDelay asc limit 1) as b 
 ON (a.dest = b.origin);

   INSERT OVERWRITE TABLE ddb_cs598_question_3_2
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

    INSERT OVERWRITE TABLE ddb_cs598_question_3_2
select *, (a.ArrDelay + b.ArrDelay) as total_delay  from 
    (select origin, dest, Carrier, FlightNum, ArrDelay, CRSDepTime, flightdate
    from cs598.cs598task1 where 
    origin = 'DFW' and
    Dest = 'ORD' and 
    Year = 2008 and
    Month = 6 and 
    DayofMonth = 10 and 
    CRSDepTime < 1200 and
    ArrDelay is not null
    sort by ArrDelay asc limit 1) as a 
JOIN 
    (select origin, dest, Carrier, FlightNum, ArrDelay, CRSDepTime, flightdate
        from cs598.cs598task1 where 
        origin = 'ORD' and
        Dest = 'DFW' and 
        Year = 2008 and
        Month = 6 and 
        DayofMonth = 12 and 
        CRSDepTime > 1200 and
        ArrDelay is not null
        sort by ArrDelay asc limit 1) as b 
 ON (a.dest = b.origin);

     INSERT OVERWRITE TABLE ddb_cs598_question_3_2
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