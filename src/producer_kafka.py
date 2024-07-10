from kafka import KafkaProducer
import os
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
orders_file_path = os.path.join(base_path, 'JSON', 'orders.json')
iventory_file_path = os.path.join(base_path, 'JSON', 'iventory.json')
notifs_file_path = os.path.join(base_path, 'JSON', 'notifs.json')

with open(orders_file_path, 'r') as file:
    transactions = json.load(file)

with open(iventory_file_path, 'r') as file:
    iventory = json.load(file)

with open(notifs_file_path, 'r') as file:
    notifs = json.load(file)

for transaction in transactions:
    producer.send('orders', transaction)
    time.sleep(1)

for item in iventory:
    producer.send('iventory', item)
    time.sleep(1)

for notif in notifs:
    producer.send('notifications', notif)
    time.sleep(1)

producer.flush()

print("Messages sent to 'orders'")

