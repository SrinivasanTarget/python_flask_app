from kafka import KafkaConsumer

consumer = KafkaConsumer('topic',
                         group_id='my_group',
                         bootstrap_servers=['localhost:9092'],
                         consumer_timeout_ms=6000)

for msg in consumer:
    print(msg.value)