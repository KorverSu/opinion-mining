import time
from src.client.redis_client import RedisClient
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
        self.__page_num = 1
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

    def visit_ebc(self):
        # 東森
        url = "https://news.ebc.net.tw/news/politics"
        self.__driver.get(url)
        while 1:
            try:
                news_list = self.__driver.find_elements(By.CLASS_NAME, "style1")
                news_list = [x for x in news_list if "white-box" in x.get_attribute("class")]
                if len(news_list) == 0:
                    print('All news have already been scraped.')
                    break
                for news in news_list:
                    tmp_driver = webdriver.Chrome(executable_path=DRIVER_PATH)
                    release_time = news.find_element(By.CLASS_NAME, "small-gray-text").text
                    ref = news.find_element(By.TAG_NAME, "a")
                    url = ref.get_attribute("href")
                    title = ref.get_attribute("title")
                    print(url)
                    print(title)
                    print(release_time)
                    tmp_driver.get(url)
                    time.sleep(2)
                    page = tmp_driver.find_element(By.CLASS_NAME, "raw-style")
                    contents = page.find_element(By.TAG_NAME, "div").text
                    print(contents)
                    tmp_driver.close()
                    # break  # for test
                butt = self.__driver.find_element(By.CLASS_NAME, "white-btn")
                self.__page_num += 1
                js = "arguments[0].setAttribute('data-page', '{}');".format(str(self.__page_num))
                self.__driver.execute_script(js, butt)
                butt.click()
                time.sleep(2)
            except Exception as e:
                print("The ebc url is invalid: ", e)

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

    def visit_ttv(self):
        # 台視
        while 1:
            try:
                url = "https://news.ttv.com.tw/category/%E6%94%BF%E6%B2%BB/{}".format(self.__page_num)
                self.__driver.get(url)
                main = self.__driver.find_element(By.TAG_NAME, "main")
                news_list = main.find_elements(By.TAG_NAME, "li")
                if len(news_list) == 0:
                    print('All news have already been scraped.')
                    break
                for news in news_list:
                    tmp_driver = webdriver.Chrome(executable_path=DRIVER_PATH)
                    url = news.find_element(By.TAG_NAME, "a").get_attribute("href")
                    title = news.find_element(By.CLASS_NAME, "title").text
                    release_time = news.find_element(By.CLASS_NAME, "time").text
                    print(url)
                    print(title)
                    print(release_time)
                    tmp_driver.get(url)
                    time.sleep(2)
                    contents = tmp_driver.find_element(By.ID, "newscontent").text
                    print(contents)
                    tmp_driver.close()
                    # break # for test
                self.__page_num += 1
            except Exception as e:
                print("The ttv url is invalid: ", e)

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
