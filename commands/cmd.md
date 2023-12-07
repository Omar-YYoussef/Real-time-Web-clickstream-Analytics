# Kafka Command Usage Guide

## For Windows:

### Zookeeper and Kafka Server Start:
In the Kafka Folder:
- Start Zookeeper server:
    ```cmd
    .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
    ```
- Start Kafka server:
    ```cmd
    .\bin\windows\kafka-server-start.bat .\config\server.properties
    ```

### Kafka Topic Operations:
In the Kafka\bin\windows Folder:
- Create a new topic:
    ```cmd
    kafka-topics.bat --create --topic Topic_Name --bootstrap-server localhost:9092
    ```
- Start a console consumer for a specific topic:
    ```cmd
    kafka-console-consumer.bat --topic Topic_Name --bootstrap-server localhost:9092 --from-beginning
    ```
- Start a console producer for a specific topic:
    ```cmd
    kafka-console-producer.bat --broker-list localhost:9092 --topic Topic_Name
    ```

## For Linux and macOS:

### Zookeeper and Kafka Server Start:
In the Kafka Folder:
- Start Zookeeper server:
    ```bash
    ./bin/zookeeper-server-start.sh ./config/zookeeper.properties
    ```
- Start Kafka server:
    ```bash
    ./bin/kafka-server-start.sh ./config/server.properties
    ```

### Kafka Topic Operations:
In the Kafka/bin Folder:
- Create a new topic:
    ```bash
    ./kafka-topics.sh --create --topic Topic_Name --bootstrap-server localhost:9092
    ```
- Start a console consumer for a specific topic:
    ```bash
    ./kafka-console-consumer.sh --topic Topic_Name --bootstrap-server localhost:9092 --from-beginning
    ```
- Start a console producer for a specific topic:
    ```bash
    ./kafka-console-producer.sh --broker-list localhost:9092 --topic Topic_Name
    ```

Replace `Topic_Name` with the desired topic name in the commands.
