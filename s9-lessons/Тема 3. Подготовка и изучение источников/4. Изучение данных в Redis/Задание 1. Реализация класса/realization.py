import json
from typing import Dict
import redis


class RedisClient:
    def __init__(self, host: str, port: int, password: str, cert_path: str) -> None:
        self._client = redis.StrictRedis(...)# Write code here
              
		def set(self, k:str, v: Dict):
				self._client ...# Write code here.

    def get(self, k:str) -> Dict:
				 self._client ...# Write code here.