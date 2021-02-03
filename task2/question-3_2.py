from __future__ import print_function

# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.2.1  --executor-memory 20G --master yarn --num-executors 50  ~/task2/question-2_3.py 

import sys
import uuid
import json
import boto3
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def save_results(items):
    dynamodb = boto3.resource('dynamodb', region_name="us-east-1")

    table = dynamodb.Table('cs598_question_3_2')
    for item in items:
        origin = item['origin']
        dest_1 = item['dest_1']
        arrDelay_1 = item['arrDelay_1']
        arrDelay_2 = item['arrDelay_2']
        carrier_1 = item['carrier_1']
        carrier_2 = item['carrier_2']
        depTime_1 = item['depTime_1']
        depTime_2 = item['depTime_2']
        dest_2 = item['dest_2']
        flightDate_1 = item['flightDate_1']
        flightDate_2 = item['flightDate_2']
        flightNum_1 = item['flightNum_1']
        flightNum_2 = item['flightNum_2']
        origin_2 = item['origin_2']
        total_delay = item['total_delay']
        table.put_item(Item=item)


def saveTopCarriers(carriers):
    sorted = carriers.sortBy(lambda item: item['total_delay'])
    for toSave in sorted.take(1):
        save_results(toSave)


if __name__ == '__main__':
    conf = SparkConf()
    conf.setAppName("Problem_3-2")
    conf.set("spark.streaming.kafka.maxRatePerPartition", 50000)
    conf.set("spark.executor.memory", "2g")
    conf.set("spark.python.worker.memory", "1g")

    # airports
    airports = ['CMI', 'ORD', 'LAX', 'JAX', 'DFW', 'CRP', 'SLC', 'BFL', 'SFO', 'PHX', 'JFK']
    year = "2008"

    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 2)
    ssc.checkpoint("/tmp/streaming")

    brokers = "b-1.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092,b-2.cs598-tast2.n69c9p.c2.kafka.us-east-1.amazonaws.com:9092"
    topic = "cs598-task2"
    kafka_consumer_group = str(uuid.uuid4())
    kafka_client_params = {
        "metadata.broker.list": brokers,
        "group.id": kafka_consumer_group,
        "auto.offset.reset": "smallest",
    }

    events = KafkaUtils.createDirectStream(ssc, [topic], kafka_client_params)
    parsed = events.map(lambda line: json.loads(line[1]))

    # filter to year 
    correct_year = parsed.filter(lambda item: item['year'] == year)

    # Filter to only airports we're looking for
    filtered_airports = correct_year.filter(lambda item: item['origin'] in airports or item['Dest'] in airports)

    flights = []

    # filter and Join for each critera
    flights.append(
        filtered_airports.filter(lambda item: item['Orgin'] == 'CMI' and item['Dest'] == 'ORD' and item['Month'] == '3' and item['DayofMonth'] == '4' and item['CRSDepTime'] < 1200) \
		.union(filtered_airports.filter(lambda item: item['Orgin'] == 'ORD' and item['Dest'] == 'DFW' and item['Month'] == '3' and item['DayofMonth'] == '6' and item['CRSDepTime'] > 1200))
    )

    flights.append(
        filtered_airports.filter(lambda item: item['Orgin'] == 'JAX' and item['Dest'] == 'DFW' and item['Month'] == '9' and item['DayofMonth'] == '9' and item['CRSDepTime'] < 1200) \
		.union(filtered_airports.filter(lambda item: item['Orgin'] == 'DFW' and item['Dest'] == 'CRP' and item['Month'] == '9' and item['DayofMonth'] == '11' and item['CRSDepTime'] > 1200))
    )

    flights.append(
        filtered_airports.filter(lambda item: item['Orgin'] == 'SLC' and item['Dest'] == 'BFL' and item['Month'] == '4' and item['DayofMonth'] == '1' and item['CRSDepTime'] < 1200) \
		.union(filtered_airports.filter(lambda item: item['Orgin'] == 'BFL' and item['Dest'] == 'LAX' and item['Month'] == '4' and item['DayofMonth'] == '3' and item['CRSDepTime'] > 1200))
    )

    flights.append(
        filtered_airports.filter(lambda item: item['Orgin'] == 'LAX' and item['Dest'] == 'SFO' and item['Month'] == '7' and item['DayofMonth'] == '12' and item['CRSDepTime'] < 1200) \
		.union(filtered_airports.filter(lambda item: item['Orgin'] == 'SFO' and item['Dest'] == 'PHX' and item['Month'] == '7' and item['DayofMonth'] == '14' and item['CRSDepTime'] > 1200))
    )

    flights.append(
        filtered_airports.filter(lambda item: item['Orgin'] == 'DFW' and item['Dest'] == 'ORD' and item['Month'] == '6' and item['DayofMonth'] == '10' and item['CRSDepTime'] < 1200) \
		.union(filtered_airports.filter(lambda item: item['Orgin'] == 'ORD' and item['Dest'] == 'DFW' and item['Month'] == '6' and item['DayofMonth'] == '12' and item['CRSDepTime'] > 1200))
    )

    flights.append(
        filtered_airports.filter(lambda item: item['Orgin'] == 'LAX' and item['Dest'] == 'ORD' and item['Month'] == '1' and item['DayofMonth'] == '1' and item['CRSDepTime'] < 1200) \
		.union(filtered_airports.filter(lambda item: item['Orgin'] == 'ORD' and item['Dest'] == 'JFK' and item['Month'] == '1' and item['DayofMonth'] == '3' and item['CRSDepTime'] > 1200))
    )

    flights.withColumn("total_delay", flights.map(lambda item: item['ArrDelay'] + item['ArrDelay_1'])

    # for each flight
    for flight in flights:
        saveTopCarriers(filtered_airports.filter(lambda item: (item['origin'], item['Dest']) == route))

    print("Completed")

    ssc.start()
    ssc.awaitTermination()