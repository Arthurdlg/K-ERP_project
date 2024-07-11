# F-5-project

# Tools to install
Docker
MongoDB

# Build project
"Install dependancies"
pip install confluent_kafka pymongo pandas

"Run Docker"
docker-compose up -build

"Build topics if not exist"
python ./src/build_topics.py

# Run
    Produce data into Kafka topic
* python ./src/producer_kafka.py *
    * Clean data *
python ./src/cons_clean.py
    * Send cleand data into database *
python ./src/store_mongo.py

# Delete docker containers
docker-compose down -v

# Project 1:
Description:
1. Build a system for K-ERP solutions to handle real-time transactions
for an e-commerce platform.
2. The data will be consumed for various operations such as inventory
management, order processing, and user notifications. So , cleaning of
the data is an important part before storing it in database.
3. The processed data should be stored in a relational database for transaction integrity.
