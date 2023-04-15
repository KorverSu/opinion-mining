from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from src.client.redis_client import RedisClient

def print_message():
    print('Hello, world!')


def a():
    print('a')


def b():
    print('b')

scheduler = BlockingScheduler()
trigger = IntervalTrigger(seconds=1)
scheduler.add_job(a, trigger)
scheduler.start()


