import time
import multiprocessing as mp
from src.client.redis_client import RedisClient
from src.client.kafka_client import KafkaClient
from src.crawler.crawler_executor import CrawlerExecutor
from src.crawler.url_collector import URLCollector
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger


class CrawlerScheduler:
    def __init__(self):
        self.__redis_cli = RedisClient()
        self.__kafka_cli = KafkaClient()
        self.__result = None
        self.__collector = None

    def get_result(self):
        return self.__result

    def run_executor(self):
        # parallel run executor #parallel programming

        while True:
            url = self.__redis_cli.set_random_pop("url")
            if url is not None:
                try:
                    if 'idle' in self.__redis_cli.fetch_all_hash_value('selenium_nodes').values():
                        idle_index = list(self.__redis_cli.fetch_all_hash_value('selenium_nodes').values()).index(
                            'idle')
                        idle_host = list(self.__redis_cli.fetch_all_hash_value('selenium_nodes').keys())[idle_index]
                        ip = idle_host.split(':')[0]
                        port = idle_host.split(':')[1]
                        self.__redis_cli.change_hash_value('selenium_nodes', idle_host, 'busy')
                        executor = CrawlerExecutor(ip, port)
                        executor.check_tv_channel(url)
                        del executor
                        self.__kafka_cli.send_log("CrawlerScheduler.run_executor", True, "run_executor success")
                    else:
                        self.__redis_cli.add_set_element('url', url)
                        # sleep for 10 second if host is busy
                        msg = "Host is busy. Sleep for 10 second."
                        self.__kafka_cli.send_log("CrawlerScheduler.run_executor", True, msg)
                        time.sleep(5)
                except Exception as e:
                    print("url is {} run_executor fail.{}: ".format(url, e))
                    error_msg = "url is {} run_executor fail.{}: ".format(url, e)
                    self.__kafka_cli.send_log("CrawlerScheduler.run_executor", False, error_msg)
            else:
                # sleep for 10 second if no url found
                print("No url found. Sleep for 10 second.")
                msg = "No url found. Sleep for 10 second."
                self.__kafka_cli.send_log("CrawlerScheduler.run_executor", True, msg)
                time.sleep(10)

    def parallel_run_executor(self, process_num=3):
        for i in range(process_num):
            p = mp.Process(target=self.run_executor())
            p.start()

    def get_popular_news(self):
        self.__collector = URLCollector()
        self.__collector.collect_new_pts_url()
        print("collect_new_pts_url finish")
        time.sleep(5)
        self.__collector.collect_new_tvbs_url()
        print("collect_new_tvbs_url finish")
        time.sleep(5)
        self.__collector.collect_new_setn_url()
        print("collect_new_setn_url finish")
        del self.__collector
        time.sleep(5)

    def collect_popular_news_per_hour(self):
        scheduler = BlockingScheduler()
        trigger = IntervalTrigger(hours=1)
        scheduler.add_job(self.get_popular_news, trigger)
        scheduler.start()


if __name__ == '__main__':
    cs = CrawlerScheduler()
    # cs.run_executor()
    # cs.collect_popular_news_per_hour()