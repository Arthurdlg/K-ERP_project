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

    def clean_data(msgs):
        required_keys = {'order_id', 'product_id', 'quantity', 'price', 'timestamp'}
        valid_msgs = [msg for msg in msgs if required_keys.issubset(msg.keys())]
        df = pd.DataFrame(valid_msgs)
        df.dropna(inplace=True)
        df = df[df['quantity'] > 0]
        df = df[df['order_id'].apply(lambda x: str(x).isdigit())]
        return df

    try:
        msg_count = 0
        msgs = []
        while True:
            msg = consumer.poll(timeout=5.0)
            print("Current message", msg)
            if msg is None:
                print("No new messages")
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            record = json.loads(msg.value().decode('utf-8'))
            print("Current record:", record)
            cleaned_df = clean_data([record])
            if not cleaned_df.empty:
                msgs.extend(cleaned_df.to_dict(orient='records'))

            msg_count += 1

            if msg_count % MIN_COMMIT_COUNT == 0:
                consumer.commit(asynchronous=False)

        if msgs:
            with open('./JSON/cleaned_data.json', 'w') as f:
                json.dump(msgs, f, indent=4)

            if msg_count % MIN_COMMIT_COUNT == 0:
                consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        pass
    finally:
        print("Closing consumer...")
        consumer.close()

if __name__ == "__main__":
    consume_and_aggregate()
