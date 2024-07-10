from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer

def topic_exists(admin_client, topic_name):
    topic_metadata = admin_client.describe_topics([topic_name])
    return topic_metadata[0]['error_code'] == 0

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='test'
)

topics_to_create = ["orders"]
existing_topics = admin_client.list_topics()

new_topics = []
for topic in topics_to_create:
    if topic not in existing_topics:
        new_topics.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))

if new_topics:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print("Topics created successfully")
else:
    print("Topics already exist")

admin_client.close()
