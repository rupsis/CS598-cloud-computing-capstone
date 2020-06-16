# import pandas as pd 

# data = pd.read_csv("2000-2005_sample.csv", encoding = "ISO-8859-1") 

# data.head()


# import chardet

# import pandas as pd

# with open('2000-2005_sample.csv', 'rb', ) as f:
#     result = chardet.detect(f.readline()) # or readline if the file is large
#     df=pd.read_csv('2000-2005_sample.csv',encoding=result['encoding'])

#     print(df.head())





import csv
import chardet

import codecs

def stripWhite(item):
    return item.strip(' "\'\t\r\n').replace(",", "")

with codecs.open('2000-2005_sample.csv', 'r', encoding='utf-8', errors='ignore') as csvfile:
    with open('2000-2005_sample_clean.csv', "w", newline='') as csvWrite:  
        csvWriter = csv.writer(csvWrite, delimiter=',', quotechar='\'', quoting=csv.QUOTE_MINIMAL)
        reader = csv.DictReader(csvfile)
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

        
