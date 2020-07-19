## Kafka
```
# download Kafka
wget https://archive.apache.org/dist/kafka/2.1.0/kafka_2.11-2.1.0.tgz

tar -xzf kafka_2.11-2.1.0.tgz


# Creating a topic
bin/kafka-topics.sh --create --zookeeper z-3.cs598-test2.9ifuoe.c2.kafka.us-east-1.amazonaws.com:2181,z-1.cs598-test2.9ifuoe.c2.kafka.us-east-1.amazonaws.com:2181,z-2.cs598-test2.9ifuoe.c2.kafka.us-east-1.amazonaws.com:2181 --replication-factor 1 --partitions 1 --topic test

# Listing Topics 
 bin/kafka-topics.sh --list --bootstrap-server z-3.cs598-test2.9ifuoe.c2.kafka.us-east-1.amazonaws.com:2181,z-1.cs598-test2.9ifuoe.c2.kafka.us-east-1.amazonaws.com:2181,z-2.cs598-test2.9ifuoe.c2.kafka.us-east-1.amazonaws.com:2181


# Producer
bin/kafka-console-producer.sh --broker-list b-2.cs598-task2.nfqkok.c2.kafka.us-east-1.amazonaws.com:9094,b-1.cs598-task2.nfqkok.c2.kafka.us-east-1.amazonaws.com:9094,b-3.cs598-task2.nfqkok.c2.kafka.us-east-1.amazonaws.com:9094 --topic AWSKafkaTutorialTopic

#consumer
bin/kafka-console-consumer.sh --bootstrap-server b-2.cs598-task2.nfqkok.c2.kafka.us-east-1.amazonaws.com:9094,b-1.cs598-task2.nfqkok.c2.kafka.us-east-1.amazonaws.com:9094,b-3.cs598-task2.nfqkok.c2.kafka.us-east-1.amazonaws.com:9094  --topic AWSKafkaTutorialTopic --from-beginning
```