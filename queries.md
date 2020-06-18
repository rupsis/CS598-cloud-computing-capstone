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