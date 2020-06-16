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
    return item.strip(' "\'\t\r\n')

with codecs.open('2000-2005_sample.csv', 'r', encoding='utf-8', errors='ignore') as csvfile:
    with open('2000-2005_sample_clean.csv', "w", newline='') as csvWrite:  
        csvWriter = csv.writer(csvWrite, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        reader = csv.DictReader(csvfile)
        for row in reader:
            # "Year","Quarter","Month","DayofMonth","DayOfWeek",
            row['Year'] = stripWhite(row['Year'])
            row['Quarter'] = stripWhite(row['Quarter'])
            row['Month'] = stripWhite(row['Month'])
            row['DayofMonth'] = stripWhite(row['DayofMonth'])
            row['DayOfWeek'] = stripWhite(row['DayOfWeek'])

            # "FlightDate","UniqueCarrier","AirlineID","Carrier",
            row['FlightDate'] = stripWhite(row['FlightDate'])
            row['UniqueCarrier'] = stripWhite(row['UniqueCarrier'])
            row['AirlineID'] = stripWhite(row['AirlineID'])
            row['Carrier'] = stripWhite(row['Carrier'])

            # "TailNum","FlightNum","Origin","OriginCityName",
            row['TailNum'] = stripWhite(row['TailNum'])
            row['FlightNum'] = stripWhite(row['FlightNum'])
            row['Origin'] = stripWhite(row['Origin'])
            row['OriginCityName'] = stripWhite(row['OriginCityName'])

            # "OriginState","OriginStateFips","OriginStateName","OriginWac"
            row['OriginState'] = stripWhite(row['OriginState'])
            row['OriginStateFips'] = stripWhite(row['OriginStateFips'])
            row['OriginStateName'] = stripWhite(row['OriginStateName'])
            row['OriginWac'] = stripWhite(row['OriginWac'])

            # "Dest", "DestCityName","DestState","DestStateFips",
            row['Dest'] = stripWhite(row['Dest'])
            row['DestCityName'] = stripWhite(row['DestCityName'])
            row['DestState'] = stripWhite(row['DestState'])
            row['DestStateFips'] = stripWhite(row['DestStateFips'])

            # "DestStateName","DestWac","CRSDepTime","DepTime",
            row['DestStateName'] = stripWhite(row['DestStateName'])
            row['DestWac'] = stripWhite(row['DestWac'])
            row['CRSDepTime'] = stripWhite(row['CRSDepTime'])
            row['DepTime'] = stripWhite(row['DepTime'])

            # "DepDelay","DepDelayMinutes","DepDel15","DepartureDelayGroups",
            row['DepDelay'] = stripWhite(row['DepDelay'])
            row['DepDelayMinutes'] = stripWhite(row['DepDelayMinutes'])
            row['DepDel15'] = stripWhite(row['DepDel15'])
            row['DepartureDelayGroups'] = stripWhite(row['DepartureDelayGroups'])

            # "DepTimeBlk","TaxiOut","WheelsOff","WheelsOn","TaxiIn",
            row['DepTimeBlk'] = stripWhite(row['DepTimeBlk'])
            row['TaxiOut'] = stripWhite(row['TaxiOut'])
            row['WheelsOff'] = stripWhite(row['WheelsOff'])
            row['WheelsOn'] = stripWhite(row['WheelsOn'])
            row['TaxiIn'] = stripWhite(row['TaxiIn'])

            # "CRSArrTime","ArrTime","ArrDelay","ArrDelayMinutes",
            row['CRSArrTime'] = stripWhite(row['CRSArrTime'])
            row['ArrTime'] = stripWhite(row['ArrTime'])
            row['ArrDelay'] = stripWhite(row['ArrDelay'])
            row['ArrDelayMinutes'] = stripWhite(row['ArrDelayMinutes'])

            # "ArrDel15","ArrivalDelayGroups","ArrTimeBlk", "Cancelled"
            row['ArrDel15'] = stripWhite(row['ArrDel15'])
            row['ArrivalDelayGroups'] = stripWhite(row['ArrivalDelayGroups'])
            row['ArrTimeBlk'] = stripWhite(row['ArrTimeBlk'])
            row['Cancelled'] = stripWhite(row['Cancelled'])

            # "CancellationCode","Diverted","CRSElapsedTime", "ActualElapsedTime"
            row['CancellationCode'] = stripWhite(row['CancellationCode'])
            row['Diverted'] = stripWhite(row['Diverted'])
            row['CRSElapsedTime'] = stripWhite(row['CRSElapsedTime'])
            row['ActualElapsedTime'] = stripWhite(row['ActualElapsedTime'])

            # "AirTime","Flights","Distance","DistanceGroup",
            row['AirTime'] = stripWhite(row['AirTime'])
            row['Flights'] = stripWhite(row['Flights'])
            row['Distance'] = stripWhite(row['Distance'])
            row['DistanceGroup'] = stripWhite(row['DistanceGroup'])

            # "CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay",
            row['CarrierDelay'] = stripWhite(row['CarrierDelay'])
            row['WeatherDelay'] = stripWhite(row['WeatherDelay'])
            row['NASDelay'] = stripWhite(row['NASDelay'])
            row['SecurityDelay'] = stripWhite(row['SecurityDelay'])
            row['LateAircraftDelay'] = stripWhite(row['LateAircraftDelay'])

        csvWriter.writerow(stripWhite(row['Year']))
