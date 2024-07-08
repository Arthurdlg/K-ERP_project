from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

transactions = [
    {"order_id": "1", "product_id": "101", "quantity": 2, "price": 300, "timestamp": "2023-07-08T10:00:00Z"},
    {"order_id": "2", "product_id": "102", "quantity": 1, "price": 150, "timestamp": "2023-07-08T10:05:00Z"},
    {"order_id": "3", "product_id": "103", "quantity": 5, "price": 100, "timestamp": "2023-07-08T10:10:00Z"}
]

for transaction in transactions:
    producer.send('orders', transaction)
    time.sleep(1)

producer.flush()

print("Messages sent to 'orders'")