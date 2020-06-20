```
USE `cs598task1-test`;

-- "Year","Quarter","Month","DayofMonth",
        -- "DayOfWeek","FlightDate","UniqueCarrier","AirlineID","Carrier",
        -- "TailNum","FlightNum","Origin","OriginCityName","OriginState",
        -- "OriginStateFips","OriginStateName","OriginWac","Dest","DestCityName",
        -- "DestState","DestStateFips","DestStateName","DestWac","CRSDepTime","DepTime",
        -- "DepDelay","DepDelayMinutes","DepDel15","DepartureDelayGroups","DepTimeBlk",
        -- "TaxiOut","WheelsOff","WheelsOn","TaxiIn","CRSArrTime","ArrTime","ArrDelay",
        -- "ArrDelayMinutes","ArrDel15","ArrivalDelayGroups","ArrTimeBlk","Cancelled",
        -- "CancellationCode","Diverted","CRSElapsedTime","ActualElapsedTime","AirTime",
        -- "Flights","Distance","DistanceGroup","CarrierDelay","WeatherDelay","NASDelay",
        -- "SecurityDelay","LateAircraftDelay"


        select Origin, count(Origin) from `cs598task1_test` GROUP BY Origin limit 10;
```


CREATE EXTERNAL TABLE ddb_cs598test 
    (id BIGINT,
    origin STRING,
    origin_total BIGINT,
    dest_total BIGINT,
    flight_total BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "cs598-test",
    "dynamodb.column.mapping"="id:id,origin:origin,origin_total:origin_total,dest_total: dest_total, flight_total:flight_total"
);



CREATE TABLE ddb_cs598test 
    (id BIGINT,
    origin STRING,
    origin_total BIGINT,
    dest_total BIGINT,
    flight_total BIGINT)
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\n';

CREATE TABLE hive_features 
    (feature_id             BIGINT,
    feature_name            STRING ,
    feature_class           STRING ,
    state_alpha             STRING,
    prim_lat_dec            DOUBLE ,
    prim_long_dec           DOUBLE ,
    elev_in_ft              BIGINT)
    ROW FORMAT DELIMITED 
    FIELDS TERMINATED BY '|'
    LINES TERMINATED BY '\n';

    INSERT OVERWRITE TABLE ddb_cs598test select Origin, count(Origin), count(Dest), count(Origin) + count(Dest) as total  from `cs598task1-test.cs598task1_test` GROUP BY Origin SORT BY total desc  limit 10;


CREATE EXTERNAL TABLE ddb_features
    (feature_id   BIGINT,
    feature_name  STRING,
    feature_class STRING,
    state_alpha   STRING,
    prim_lat_dec  DOUBLE,
    prim_long_dec DOUBLE,
    elev_in_ft    BIGINT)
STORED BY 'org.apache.hadoop.hive.dynamodb.DynamoDBStorageHandler'
TBLPROPERTIES(
    "dynamodb.table.name" = "Features",
    "dynamodb.column.mapping"="feature_id:Id,feature_name:Name,feature_class:Class,state_alpha:State,prim_lat_dec:Latitude,prim_long_dec:Longitude,elev_in_ft:Elevation"
);