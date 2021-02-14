# Kafka Twitter
## Quick start
1. run zookeeper `zookeeper-server-start.sh path/to/config/folder/zookeeper.properties`
2. run broker `kafka-server-start.sh path/to/config/folder/server.properties`
3. run console consumer `kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic <topic name>`
4. run TwitterProducer
