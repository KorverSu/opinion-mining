import random
import time

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from src.config import DRIVER_PATH, PTS_NUM, TVBS_NUM, SETN_NUM, SELENIUM_IP, SELENIUM_PORT
from src.client.redis_client import RedisClient
from src.client.kafka_client import KafkaClient
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


class URLCollector:
    def __init__(self, ip=SELENIUM_IP, port=SELENIUM_PORT):
        self.__redis_cli = RedisClient()
        self.__kafka_cli = KafkaClient()
        # self.__driver = webdriver.Chrome(executable_path=DRIVER_PATH)
        self.__driver = webdriver.Remote(
            command_executor='http://{}:{}'.format(ip, port),
            desired_capabilities={'browserName': 'chrome', 'javascriptEnabled': True})
        self.__page_num = 1

    def collect_old_url_by_id(self, tv_station: str):
        # tvbs新聞數量大概有 2000000
        station_dic = {
            "tvbs": (TVBS_NUM, "https://news.tvbs.com.tw/politics/{}"),
            "setn": (SETN_NUM, "https://www.setn.com/News.aspx?NewsID={}"),
            "pts": (PTS_NUM, "https://news.pts.org.tw/article/{}")
        }
        if tv_station not in station_dic.keys():
            error_msg = "{} is unsupported TV station".format(tv_station)
            print(error_msg)
            self.__kafka_cli.send_log("URLCollector.collect_old_url_by_id", False, error_msg)
            return
        total = station_dic.get(tv_station)[0]
        for news_id in range(total):
            try:
                url = station_dic.get(tv_station)[1].format(news_id)
                self.__redis_cli.add_set_element('url', url)
                msg = "add {} to redis key url success".format(url)
                print(msg)
                self.__kafka_cli.send_log("URLCollector.collect_old_url_by_id", True, msg)
                time.sleep(1)
            except Exception as e:
                error_msg = "collect_old_url_by_id fail. Error is: {}".format(e)
                self.__kafka_cli.send_log("URLCollector.collect_old_url_by_id", False, error_msg)

    def collect_ebc_url(self):
        # 東森
        source_url = "https://news.ebc.net.tw/news/politics"
        self.__driver.get(source_url)
        while 1:
            try:
                # 等待 By.CLASS_NAME 為 style1 的元素出現
                WebDriverWait(self.__driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "style1")))
                news_list = self.__driver.find_elements(By.CLASS_NAME, "style1")
                news_list = [x for x in news_list if "white-box" in x.get_attribute("class")]
                if len(news_list) == 0:
                    msg = 'All news have already been scraped.'
                    self.__kafka_cli.send_log("URLCollector.collect_ebc_url", True, msg)
                    return
                for news in news_list:
                    release_time = news.find_element(By.CLASS_NAME, "small-gray-text").text
                    ref = news.find_element(By.TAG_NAME, "a")
                    url = ref.get_attribute("href")
                    self.__redis_cli.add_set_element('url', url)
                    msg = "add {} to redis key url success".format(url)
                    self.__kafka_cli.send_log("URLCollector.collect_ebc_url", True, msg)

                WebDriverWait(self.__driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "white-btn")))
                self.__driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                butt = self.__driver.find_element(By.CLASS_NAME, "white-btn")
                self.__page_num += 1
                js = "arguments[0].setAttribute('data-page', '{}');".format(str(self.__page_num))
                self.__driver.execute_script(js, butt)
                butt.click()
                time.sleep(random.randint(1, 3))
            except Exception as e:
                error_msg = "collect_ebc_url fail. Error is: {}".format(e)
                self.__kafka_cli.send_log("URLCollector.collect_ebc_url", False, error_msg)

    def collect_ttv_url(self):
        # 台視
        while 1:
            try:
                source_url = "https://news.ttv.com.tw/category/%E6%94%BF%E6%B2%BB/{}".format(self.__page_num)
                self.__driver.get(source_url)
                WebDriverWait(self.__driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "main")))
                main = self.__driver.find_element(By.TAG_NAME, "main")
                news_list = main.find_elements(By.TAG_NAME, "li")
                if len(news_list) == 0:
                    msg = 'All news have already been scraped.'
                    self.__kafka_cli.send_log("URLCollector.collect_ttv_url", True, msg)
                    return
                for news in news_list:
                    url = news.find_element(By.TAG_NAME, "a").get_attribute("href")
                    self.__redis_cli.add_set_element('url', url)
                    msg = "add {} to redis key url success".format(url)
                    self.__kafka_cli.send_log("URLCollector.collect_ttv_url", True, msg)
                time.sleep(random.randint(1, 3))
                self.__page_num += 1
            except Exception as e:
                error_msg = "collect_ttv_url fail. Error is: {}".format(e)
                self.__kafka_cli.send_log("URLCollector.collect_ttv_url", False, error_msg)

    def collect_new_tvbs_url(self):
        # 獲取前10熱門的新聞
        source_url = "https://news.tvbs.com.tw/"
        try:
            self.__driver.get(source_url)
            WebDriverWait(self.__driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "article_rank")))
            article_rank = self.__driver.find_element(By.CLASS_NAME, "article_rank")
            news_list = article_rank.find_elements(By.TAG_NAME, "li")
            for news in news_list:
                url = news.find_element(By.TAG_NAME, "a").get_attribute("href")
                # title = news.find_element(By.CLASS_NAME, "txt").text
                self.__redis_cli.add_set_element('url', url)
                msg = "add {} to redis key url success".format(url)
                self.__kafka_cli.send_log("URLCollector.collect_new_tvbs_url", True, msg)
        except Exception as e:
            error_msg = "collect_new_tvbs_url fail. Error is: {}".format(e)
            self.__kafka_cli.send_log("URLCollector.collect_new_tvbs_url", False, error_msg)

    def collect_new_setn_url(self):
        # 獲取前10熱門的新聞
        source_url = "https://www.setn.com/"
        try:
            self.__driver.get(source_url)
            WebDriverWait(self.__driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "top-hot-list")))
            top_hot_list = self.__driver.find_element(By.CLASS_NAME, "top-hot-list")
            news_list = top_hot_list.find_elements(By.TAG_NAME, "li")
            for news in news_list:
                url = news.find_element(By.TAG_NAME, "a").get_attribute("href")
                # title = news.find_element(By.TAG_NAME, "a").text
                self.__redis_cli.add_set_element('url', url)
                msg = "add {} to redis key url success".format(url)
                self.__kafka_cli.send_log("URLCollector.collect_new_setn_url", True, msg)
        except Exception as e:
            error_msg = "collect_new_setn_url fail. Error is: {}".format(e)
            self.__kafka_cli.send_log("URLCollector.collect_new_setn_url", False, error_msg)

    def collect_new_pts_url(self):
        # 獲取前10熱門的新聞
        source_url = "https://news.pts.org.tw/"
        try:
            self.__driver.get(source_url)
            WebDriverWait(self.__driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "w-box")))
            w_box = self.__driver.find_element(By.CLASS_NAME, "w-box")
            news_list = w_box.find_elements(By.TAG_NAME, "a")
            print(len(news_list))
            for news in news_list:
                url = news.get_attribute("href")
                title = news.text
                if not title:
                    continue
                self.__redis_cli.add_set_element('url', url)
                msg = "add {} to redis key url success".format(url)
                self.__kafka_cli.send_log("URLCollector.collect_new_pts_url", True, msg)

        except Exception as e:
            error_msg = "collect_new_setn_url fail. Error is: {}".format(e)
            self.__kafka_cli.send_log("URLCollector.collect_new_pts_url", False, error_msg)

    def __del__(self):
        self.__driver.quit()


if __name__ == '__main__':
    uc = URLCollector()
    uc.collect_new_pts_url()
    print("finish")
