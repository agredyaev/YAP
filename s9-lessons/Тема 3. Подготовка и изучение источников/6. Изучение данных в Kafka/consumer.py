from confluent_kafka import Consumer
import os

TOPIC_NAME = "order-service_orders"
KAFKA_HOST = os.environ["YAP_KAFKA_HOST"]
KAFKA_PORT = 9091


def error_callback(err):
    print("Something went wrong: {}".format(err))


params = {
    "bootstrap.servers": f"{KAFKA_HOST}:{KAFKA_PORT}",
    "security.protocol": "SASL_SSL",
    "ssl.ca.location": "/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "producer_consumer",
    "sasl.password": os.environ["YAP_KAFKA_PASS"],
    "group.id": "test-consumer1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "error_cb": error_callback,
    "debug": "all",
}
c = Consumer(params)
c.subscribe([TOPIC_NAME])
while True:
    msg = c.poll(timeout=3.0)
    if msg:
        val = msg.value().decode()
        print(val)
