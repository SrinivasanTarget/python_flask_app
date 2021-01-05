from kafka import KafkaConsumer

consumer = KafkaConsumer('my-topic',
                         group_id='my_group',
                         bootstrap_servers=['localhost:9092'], consumer_timeout_ms=1000)

for msg in consumer:
    print(msg.value)