#!/usr/bin/env bash
cd ~/Documents/workspace-oxygen/data/stream-reactor
./gradlew :kafka-connect-hive:shadowJar
echo "kafka-connect-hive jar was packaged"
cd kafka-connect-hive/build/libs/
cp kafka-connect-hive-1.2.1-2.1.0-all.jar /Users/pengwang/Documents/TestProject/kafka/confluent-5.1.0/plugins/lib/
