from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

producer.send('topic', b'Welcome to 2021')

producer.flush()
producer.close()