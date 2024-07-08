#!/bin/bash

if ! docker-compose ps | grep -q kafka-1; then
    echo "Kafka container not working."
    exit 1
fi

JSON_FILE="orders.json"

if [[ ! -f "$JSON_FILE" ]]; then
    echo "File $JSON_FILE does not exist."
    exit 1
fi

# Produce messages to the 'orders' topic
echo "Sending messages to the 'orders' topic..."

# Read the JSON file line by line and send each line as a message to the 'orders' topic
while IFS= read -r line
do
    echo "$line" | docker exec -i kafka-1 kafka-console-producer.sh --broker-list localhost:9092 --topic orders
done < <(jq -c '.[]' "$JSON_FILE")

echo "Messages sent successfully."
