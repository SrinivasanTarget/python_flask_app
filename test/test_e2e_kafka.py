from kafka import KafkaProducer
from kafka import KafkaConsumer
from test.kafka import KafkaContainer

def test_e2e_kafka():
    with KafkaContainer() as kafka:
        producer = KafkaProducer(bootstrap_servers=[kafka.get_bootstrap_servers()])

        consumer = KafkaConsumer('test',
                        group_id='my_group',
                        bootstrap_servers=[kafka.get_bootstrap_servers()],
                        consumer_timeout_ms=9000)
        for msg in consumer:
            assert msg.value == 'Im Srinivasan Sekar'