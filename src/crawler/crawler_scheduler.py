import time

from src.client.redis_client import RedisClient


class CrawlerScheduler:
    def __init__(self):
        self.__redis_cli = RedisClient()

    def run_executor(self):
        while True:
            url = self.__redis_cli.set_random_pop("url")
            if url is not None:
                try:

                    driver.get(url)

                except Exception as e:
                    print("Error: ", e)
            else:
                # sleep for 1 second if no url found
                time.sleep(10)
