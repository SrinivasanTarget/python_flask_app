from kafka import KafkaProducer

from test.kafka import KafkaContainer


def test_users():
    with KafkaContainer() as kafka:
        producer = KafkaProducer(bootstrap_servers=[kafka.get_bootstrap_servers()])

        producer.send('my-topic', b'Im Srinivasan Sekar')

        producer.flush()
