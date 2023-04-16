import time
from src.client.redis_client import RedisClient
from src.client.kafka_client import KafkaClient
from selenium.webdriver.common.by import By
from selenium import webdriver
from src.config import DRIVER_PATH, SELENIUM_IP, SELENIUM_PORT


class CrawlerExecutor:
    def __init__(self, ip=SELENIUM_IP, port=SELENIUM_PORT):
        self.__host = "{}:{}".format(ip, port)
        self.__redis_cli = RedisClient()
        self.__driver = webdriver.Remote(
            command_executor='http://{}:{}'.format(ip, port),
            desired_capabilities={'browserName': 'chrome', 'javascriptEnabled': True})
        self.__result = None

    def get_result(self):
        return self.__result

    def crawl_tvbs(self, source_url: str):
        # id ~ 2000000
        try:
            self.__driver.get(source_url)
            title_box = self.__driver.find_element(By.CLASS_NAME, "title_box")
            title = title_box.find_element(By.CLASS_NAME, "title").text
            author = self.__driver.find_element(By.CLASS_NAME, "author")
            contents = self.__driver.find_element(By.CLASS_NAME, "article_content").text
            time_list = author.text.split("\n")
            release_time = time_list[1].strip("發佈時間：")
            last_updated = time_list[2].strip("最後更新時間：")
            result = {
                "title": title,
                "release_time": last_updated,
                "contents": contents
            }
            self.__result = result
            print(result)
        except Exception as e:
            print("crawl_tvbs fail. Error is: ", e)
            self.__result = None

    def crawl_setn(self, source_url: str):
        # 三立新聞 id ~ 1300000
        try:
            self.__driver.get(source_url)
            title = self.__driver.find_element(By.CLASS_NAME, "news-title-3").text
            release_time = self.__driver.find_element(By.CLASS_NAME, "page-date").text
            contents = self.__driver.find_element(By.TAG_NAME, "article").text
            result = {
                "title": title,
                "release_time": release_time,
                "contents": contents
            }
            self.__result = result
            print(result)
        except Exception as e:
            print("crawl_setn fail. Error is: ", e)
            self.__result = None

    def crawl_ebc(self, source_url: str):
        try:
            self.__driver.get(source_url)
            fncnews_content = self.__driver.find_element(By.CLASS_NAME, "fncnews-content")
            title = fncnews_content.find_element(By.TAG_NAME, 'h1').text
            release_time = fncnews_content.find_element(By.CLASS_NAME, 'small-gray-text').text
            page = fncnews_content.find_element(By.CLASS_NAME, "raw-style")
            contents = page.find_element(By.TAG_NAME, "div").text
            result = {
                "title": title,
                "release_time": release_time,
                "contents": contents
            }
            self.__result = result
            print(result)
        except Exception as e:
            print('crawl_ebc fail. Error is: ', e)
            self.__result = None

    def crawl_ttv(self, source_url: str):
        try:
            self.__driver.get(source_url)
            title = self.__driver.find_element(By.CLASS_NAME, "mb-ht-hf").text
            release_time = self.__driver.find_element(By.XPATH, '/html/body/form/main/div[2]/ul/li').text
            contents = self.__driver.find_element(By.ID, "newscontent").text
            result = {
                "title": title,
                "release_time": release_time,
                "contents": contents
            }
            self.__result = result
            print(result)
        except Exception as e:
            print('crawl_ttv fail. Error is: ', e)
            self.__result = None

    def crawl_pts(self, source_url: str):
        # 公視 id ~ 629549
        try:
            self.__driver.get(source_url)
            title = self.__driver.find_element(By.CLASS_NAME, "article-title").text
            time_list = self.__driver.find_elements(By.CLASS_NAME, "text-nowrap")
            release_time = time_list[0].find_element(By.TAG_NAME, "time").text
            last_updated = time_list[1].find_element(By.TAG_NAME, "time").text
            contents = self.__driver.find_element(By.CLASS_NAME, "post-article").text
            result = {
                "title": title,
                "release_time": last_updated,
                "contents": contents
            }
            self.__result = result
            print(result)
        except Exception as e:
            print('crawl_pts fail. Error is: ', e)
            self.__result = None

    def check_tv_channel(self, source_url: str):
        if 'tvbs' in source_url:
            self.crawl_tvbs(source_url)
        elif 'setn' in source_url:
            self.crawl_setn(source_url)
        elif 'ebc' in source_url:
            self.crawl_ebc(source_url)
        elif 'ttv' in source_url:
            self.crawl_ttv(source_url)
        elif 'pts' in source_url:
            self.crawl_pts(source_url)
        else:
            print('The url is invalid in check_tv_channel.')

    def change_to_idle(self):
        try:
            self.__redis_cli.change_hash_value('selenium_nodes', self.__host, 'idle')
            print('change to idle')
        except Exception as e:
            print('change to idle fail. Error is: ', e)

    def __del__(self):
        self.change_to_idle()
        self.__driver.quit()


if __name__ == '__main__':
    #  pass
    ce = CrawlerExecutor()
    ce.crawl_tvbs('https://news.tvbs.com.tw/politics/1')
    # ce.crawl_setn('https://www.setn.com/News.aspx?NewsID=10000')
