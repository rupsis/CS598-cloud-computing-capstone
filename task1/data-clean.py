import csv
import chardet
import sys
import codecs

def stripWhite(item):
    return item.strip(' "\'\t\r\n').replace(",", "")

with codecs.open(sys.argv[1], 'r', encoding='utf-8', errors='ignore') as csvfile:
    with open(sys.argv[2], "w", newline='') as csvWrite:  
        csvWriter = csv.writer(csvWrite, delimiter=',', quotechar='\'', quoting=csv.QUOTE_MINIMAL)
        reader = csv.DictReader(csvfile)
        csvWriter.writerow(["Year","Quarter","Month","DayofMonth",
        "DayOfWeek","FlightDate","UniqueCarrier","AirlineID","Carrier",
        "TailNum","FlightNum","Origin","OriginCityName","OriginState",
        "OriginStateFips","OriginStateName","OriginWac","Dest","DestCityName",
        "DestState","DestStateFips","DestStateName","DestWac","CRSDepTime","DepTime",
        "DepDelay","DepDelayMinutes","DepDel15","DepartureDelayGroups","DepTimeBlk",
        "TaxiOut","WheelsOff","WheelsOn","TaxiIn","CRSArrTime","ArrTime","ArrDelay",
        "ArrDelayMinutes","ArrDel15","ArrivalDelayGroups","ArrTimeBlk","Cancelled",
        "CancellationCode","Diverted","CRSElapsedTime","ActualElapsedTime","AirTime",
        "Flights","Distance","DistanceGroup","CarrierDelay","WeatherDelay","NASDelay",
        "SecurityDelay","LateAircraftDelay",])


        for row in reader:
            # "Year","Quarter","Month","DayofMonth","DayOfWeek",
            csvWriter.writerow([
            stripWhite(row['Year']),
            stripWhite(row['Quarter']),
            stripWhite(row['Month']),
            stripWhite(row['DayofMonth']),
            stripWhite(row['DayOfWeek']),

            # "FlightDate","UniqueCarrier","AirlineID","Carrier",
            stripWhite(row['FlightDate']),
            stripWhite(row['UniqueCarrier']),
            stripWhite(row['AirlineID']),
            stripWhite(row['Carrier']),

            # "TailNum","FlightNum","Origin","OriginCityName",
            stripWhite(row['TailNum']),
            stripWhite(row['FlightNum']),
            stripWhite(row['Origin']),
            stripWhite(row['OriginCityName']),

            # "OriginState","OriginStateFips","OriginStateName","OriginWac"
            stripWhite(row['OriginState']),
            stripWhite(row['OriginStateFips']),
            stripWhite(row['OriginStateName']),
            stripWhite(row['OriginWac']),

            # "Dest", "DestCityName","DestState","DestStateFips",
            stripWhite(row['Dest']),
            stripWhite(row['DestCityName']),
            stripWhite(row['DestState']),
            stripWhite(row['DestStateFips']),

            # "DestStateName","DestWac","CRSDepTime","DepTime",
            stripWhite(row['DestStateName']),
            stripWhite(row['DestWac']),
            stripWhite(row['CRSDepTime']),
            stripWhite(row['DepTime']),

            # "DepDelay","DepDelayMinutes","DepDel15","DepartureDelayGroups",
            stripWhite(row['DepDelay']),
            stripWhite(row['DepDelayMinutes']),
            stripWhite(row['DepDel15']),
            stripWhite(row['DepartureDelayGroups']),

            # "DepTimeBlk","TaxiOut","WheelsOff","WheelsOn","TaxiIn",
            stripWhite(row['DepTimeBlk']),
            stripWhite(row['TaxiOut']),
            stripWhite(row['WheelsOff']),
            stripWhite(row['WheelsOn']),
            stripWhite(row['TaxiIn']),

            # "CRSArrTime","ArrTime","ArrDelay","ArrDelayMinutes",
            stripWhite(row['CRSArrTime']),
            stripWhite(row['ArrTime']),
            stripWhite(row['ArrDelay']),
            stripWhite(row['ArrDelayMinutes']),

            # "ArrDel15","ArrivalDelayGroups","ArrTimeBlk", "Cancelled"
            stripWhite(row['ArrDel15']),
            stripWhite(row['ArrivalDelayGroups']),
            stripWhite(row['ArrTimeBlk']),
            stripWhite(row['Cancelled']),

            # "CancellationCode","Diverted","CRSElapsedTime", "ActualElapsedTime"
            stripWhite(row['CancellationCode']),
            stripWhite(row['Diverted']),
            stripWhite(row['CRSElapsedTime']),
            stripWhite(row['ActualElapsedTime']),

            # "AirTime","Flights","Distance","DistanceGroup",
            stripWhite(row['AirTime']),
            stripWhite(row['Flights']),
            stripWhite(row['Distance']),
            stripWhite(row['DistanceGroup']),

            # "CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay",
            stripWhite(row['CarrierDelay']),
            stripWhite(row['WeatherDelay']),
            stripWhite(row['NASDelay']),
            stripWhite(row['SecurityDelay']),
            stripWhite(row['LateAircraftDelay'])])

        
