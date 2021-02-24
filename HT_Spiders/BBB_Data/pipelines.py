import os

import pymongo
from pymongo import MongoClient
from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter
from scrapy.logformatter import LogFormatter


class CleanItems:
    def __init__(self):
        self.urls_seen = set()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        
        if adapter["company_name"] == None:
            raise DropItem(
                f"No name detected. Dropping {item['bbb_url'].split('/')[-1]}"
            )
        elif len(adapter["zip_code"]) != 5:
            bad_zip = adapter.pop("zip_code")
        else:
            # self.urls_seen.add(adapter['bbb_url'])
            return item


class DropDuplicateURLS:
    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client[os.environ["mongo_db"]]
        self.coll = self.db["ProDataCollection"]

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        pro_in_db = self.coll.find_one({"bbb_url": adapter["bbb_url"]})

        spider.logger.info(
            self.coll.update({"_id": pro_in_db["_id"]}, {"$set": dict(adapter)})
        )

        spider.logger.info(
            self.coll.remove(
                {"_id": {"$ne": pro_in_db["_id"]}, "bbb_url": item["bbb_url"]},
                multi=True,
            )
        )

        return item


class DropNoneValues:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        for key in list(adapter.keys()):
            if adapter[key] == "None":
                bad_key = adapter.pop(key)

        return item


class MongoPipeline:
    collection_name = "test_pros"

    def __init__(self):
        self.client = pymongo.MongoClient()
        self.db = self.client[os.environ["mongo_db"]]
        self.coll = self.db["ProDataCollection"]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):

        adapter = ItemAdapter(item)
        update = self.coll.update(
            {"bbb_url": adapter["bbb_url"]},
            {"$set": dict(adapter)},
            upsert=False,
        )
        spider.logger.info(f"Updated {item['bbb_url'].split('/')[-1]} in the database!")
        spider.logger.info(f"{update}")

        return item
