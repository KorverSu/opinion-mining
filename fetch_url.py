from src.crawler.crawler_scheduler import CrawlerScheduler

if __name__ == '__main__':
    crawler_scheduler = CrawlerScheduler()
    crawler_scheduler.collect_popular_news_per_hour()