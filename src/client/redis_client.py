import time

import redis
from src.config import REDIS_HOST, REDIS_PORT


class RedisClient:
    def __init__(self):
        self.__r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    def add_set_element(self, key: str, value: str):
        try:
            self.__r.sadd(key, value)
        except Exception as e:
            print("Add to key {} fail value is {}\nError is: {}".format(key, value, e))

    def delete_set_element(self, key: str, value: str):
        try:
            self.__r.srem(key, value)
        except Exception as e:
            print("Delete from key {} fail value is {}\nError is: {}".format(key, value, e))

    def fetch_set_elements(self, key: str):
        try:
            elements = self.__r.smembers(key)
            return elements
        except Exception as e:
            print("Fetch {} elements fail\nError is: {}".format(key, e))

    def fetch_set_elements_count(self, key: str):
        self.__r.close()
        try:
            count = self.__r.scard(key)
            return count
        except Exception as e:
            print("Fetch {} count fail\nError is: {}".format(key, e))

    def set_random_pop(self, key: str):
        try:
            element = self.__r.spop(key)
            return element
        except Exception as e:
            print("Random pop element from {} fail\nError is: {}".format(key, e))

    def is_set_element(self, key: str, value: str):
        try:
            return self.__r.sismember(key, value)
        except Exception as e:
            print("is_set_element fail Error is: {}", e)

    def add_hash_value(self, name: str, key: str, value: str):
        """
        name selenium_nodes
        key nodes:ip
        value idle or busy
        """
        try:
            self.__r.hset(name, key, value)
        except Exception as e:
            print("add_hash_value fail Error is: {}", e)

    def fetch_hash_value(self, name: str, key: str):
        try:
            return self.__r.hget(name, key)
        except Exception as e:
            print("fetch_hash_value fail Error is: {}", e)

    def fetch_all_hash_value(self, name: str):
        try:
            return self.__r.hgetall(name)
        except Exception as e:
            print("fetch_all_hash_value fail Error is: {}", e)

    def change_hash_value(self, name: str, key: str, value: str):
        with self.__r.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(name)
                    pipe.multi()
                    pipe.hset(name, key, value)
                    pipe.execute()
                    break
                except redis.WatchError:
                    time.sleep(3)
                    print("The selenium_nodes state already changed, retry")
                    continue

    def __del__(self):
        self.__r.close()
