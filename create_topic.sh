#/bin/bash

kafka-topics --create --if-not-exists --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1 --topic $TOPIC_NAME
echo "Topic $TOPIC_NAME created!"