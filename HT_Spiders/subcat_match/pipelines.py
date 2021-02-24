import os

import pymongo
from pymongo import MongoClient
from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter
from scrapy.logformatter import LogFormatter


class MongoPipeline:
    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client[os.environ["mongo_db"]]
        self.coll = self.db["CorpusCollection"]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):

        adapter = ItemAdapter(item)
        update = self.coll.update_one(
            {"_id": adapter["mongoid"]},
            {"$set": dict(adapter)},
        )

        spider.logger.info(f"{update}")

        return item


class DropDuplicateURLS:
    def __init__(self):
        self.seen_urls = []

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter["bbb_url"] in self.seen_urls:
            DropItem(adapter)

        spider.logger.info(f"duplicate found {adapter['bbb_url']}")

        return item
