from confluent_kafka import Producer
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
    "error_cb": error_callback,
}

p = Producer(params)
p.produce(
    TOPIC_NAME,
    ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",
)
# p.flush(10)
