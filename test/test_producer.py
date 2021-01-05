from kafka import KafkaProducer

def test_users(kafka):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    producer.send('my-topic', b'Im Srinivasan Sekar')

    producer.flush()