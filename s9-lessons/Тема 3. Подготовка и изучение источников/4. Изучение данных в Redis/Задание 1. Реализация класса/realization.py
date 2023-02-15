import json
from typing import Dict
import redis


class RedisClient:
    def __init__(self, host: str, port: int, password: str, cert_path: str):
        self._client = redis.StrictRedis(
            host=host, port=port, password=password, ssl=True, ssl_ca_certs=cert_path
        )

    def set(self, k: str, v: Dict):
        self._client.set(k, json.dumps(v))

    def get(self, k: str) -> Dict:
        obj = self._client.get(k)
        return json.loads(obj)
