from src.crawler.crawler_scheduler import CrawlerScheduler

if __name__ == '__main__':
    crawler_scheduler = CrawlerScheduler()
    crawler_scheduler.parallel_run_executor()
