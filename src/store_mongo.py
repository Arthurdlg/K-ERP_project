import os
import json
from pymongo import MongoClient

host = "localhost"
port = 27017
database = "ecommerce"
client = MongoClient(host, port)
db = client[database]

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
agg_json_file_path = os.path.join(base_path, 'JSON', 'cleaned_data.json')

def store_in_mongodb():
    with open(agg_json_file_path, 'r') as file:
        data = json.load(file)
        if data:
            db.agg_result.insert_many(data)

if __name__ == "__main__":
    store_in_mongodb()
