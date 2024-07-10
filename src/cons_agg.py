import json
import pandas as pd
from confluent_kafka import Consumer, KafkaError

MIN_COMMIT_COUNT = 1000

kafka_consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

def consume_and_aggregate():
    consumer = Consumer(kafka_consumer_config)
    consumer.subscribe(['orders'])
    
    print("Consuming messages...")

    def aggregation_basic(msgs):
        df = pd.DataFrame(msgs)
        aggDF = df.groupby("product_id").sum()
        aggDF.reset_index(inplace=True)
        aggDF.to_json('aggregated_data.json', orient='records')

    try:
        msg_count = 0
        msgs = []
        while True:
            msg = consumer.poll(timeout=1.0)
            print("Current message", msg)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            record = json.loads(msg.value().decode('utf-8'))
            msgs.append(record)
            msg_count += 1

            if msg_count % 10 == 0:
                aggregation_basic(msgs)
                msgs = []

            if msg_count % MIN_COMMIT_COUNT == 0:
                consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        print("Closing consumer...")
        consumer.close()

if __name__ == "__main__":
    consume_and_aggregate()
