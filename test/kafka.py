import tarfile
import time
from io import BytesIO

import kafka

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready


class KafkaContainer(DockerContainer):
    KAFKA_PORT = 9093
    TC_START_SCRIPT = '/tc-start.sh'

    def __init__(self, image="confluentinc/cp-kafka:5.4.3", port_to_expose=KAFKA_PORT):
        super(KafkaContainer, self).__init__(image)
        self.port_to_expose = port_to_expose
        self.with_exposed_ports(self.port_to_expose)
        self.with_env('KAFKA_LISTENERS', 'PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092')
        self.with_env('KAFKA_LISTENER_SECURITY_PROTOCOL_MAP', 'BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT')
        self.with_env('KAFKA_INTER_BROKER_LISTENER_NAME', 'BROKER')

        self.with_env('KAFKA_BROKER_ID', '1')
        self.with_env('KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR', '1')
        self.with_env('KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS', '1')
        self.with_env('KAFKA_LOG_FLUSH_INTERVAL_MESSAGES', '10000000')
        self.with_env('KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS', '0')

    def get_bootstrap_servers(self):
        return '{}:{}'.format(self.get_container_host_ip(), self.get_exposed_port(KafkaContainer.KAFKA_PORT))

    @wait_container_is_ready()
    def _connect(self):
        consumer = kafka.KafkaConsumer(group_id='test', bootstrap_servers=[self.get_bootstrap_servers()])
        topics = consumer.topics()
        if not topics:
            raise Exception()

    def tc_start(self):
        port = self.get_exposed_port(KafkaContainer.KAFKA_PORT)
        data = f"""#!/bin/bash
echo 'clientPort=2181' > zookeeper.properties
echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties
echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties
zookeeper-server-start zookeeper.properties &
export KAFKA_ZOOKEEPER_CONNECT='localhost:2181'
export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:{port},BROKER://$(hostname -i):9092
. /etc/confluent/docker/bash-config
/etc/confluent/docker/configure
/etc/confluent/docker/launch""".encode('utf-8')

        tar_stream = BytesIO()
        tar = tarfile.TarFile(fileobj=tar_stream, mode='w')
        tarinfo = tarfile.TarInfo(name=KafkaContainer.TC_START_SCRIPT)
        tarinfo.size = len(data)
        tarinfo.mtime = time.time()
        tar.addfile(tarinfo, BytesIO(data))
        tar.close()
        tar_stream.seek(0)
        self.get_wrapped_container().put_archive('/', tar_stream)

    def start(self):
        self.with_command(f'sh -c "while [ ! -f {KafkaContainer.TC_START_SCRIPT} ]; do sleep 0.1; done; '
                          f'sh {KafkaContainer.TC_START_SCRIPT}"')
        super().start()
        self.tc_start()
        self._connect()
        return self
