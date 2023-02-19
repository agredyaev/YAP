import time
from datetime import datetime
from logging import Logger
import json
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis import RedisClient
from stg_loader.repository import StgRepository


class StgMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        redis_client: RedisClient,
        stg_repository: StgRepository,
        batch_size: int,
        logger: Logger,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis_client
        self._stg_repository = stg_repository
        self._logger = logger
        self._batch_size = batch_size

    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if not msg:
                break

            object_id = msg["object_id"]
            payload = msg["payload"]
            sent_dttm = msg["sent_dttm"]

            self._logger.info(
                f"{datetime.utcnow()}: #{_} message {object_id} is recieved"
            )

            self._stg_repository.order_events_insert(
                object_id=object_id,
                object_type=msg["object_type"],
                sent_dttm=sent_dttm,
                payload=json.dumps(payload),
            )

            user_id = payload["user"]["id"]
            user = self._redis.get(user_id)
            user_name = user["name"]
            user_login = user["login"]

            restaurant_id = payload["restaurant"]["id"]
            restaurant = self._redis.get(restaurant_id)
            restaurant_name = restaurant["name"]

            output = {
                "object_id": object_id,
                "object_type": "order",
                "payload": {
                    "id": msg["object_id"],
                    "date": payload["date"],
                    "cost": payload["cost"],
                    "payment": payload["payment"],
                    "status": payload["final_status"],
                    "restaurant": {"id": restaurant_id, "name": restaurant_name},
                    "user": {"id": user_id, "name": user_name, "login": user_login},
                    "products": payload["order_items"],
                },
            }

            self._producer.produce(output)

            self._logger.info(f"{datetime.utcnow()}: #{_} message {object_id} is sent")

        self._logger.info(f"{datetime.utcnow()}: FINISH")
