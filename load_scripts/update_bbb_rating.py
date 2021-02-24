import os, datetime

# importing module
import logging

import pymongo
from pymongo import MongoClient
import pandas as pd

from func import bbb_rating_mapping, to_dict, session_scope
from db import Base

# Create n configure logger
logging.basicConfig(
    filename="update_bbb_rating.log", format="%(message)s", filemode="w"
)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# map company table
Company = Base.classes["companies_company"]

## connect to pymongo
client = MongoClient()
db = client["ProData"]
pro_coll = db["ProDataCollection"]

url_ratings = [
    dict(x)
    for x in pro_coll.find(
        {"in_production": True, "bbb_rating": {"$nin": [None, "None"]}},
        {"_id": False, "bbb_url": True, "bbb_rating": True},
    )
]

for i in url_ratings:
    with session_scope() as session:
        query = list(
            session.query(Company)
            .filter(Company.bbb_url == i["bbb_url"])
            .filter(Company.bbb_rating == None)
            .all()
        )

        if len(query) == 0:
            logger.info(f"{i['bbb_url']} is not in prod db or has already been added")
        elif len(query) == 1:
            logger.info(f"{i['bbb_url']} has updated rating!")
            company = query[0]
            company.bbb_rating = bbb_rating_mapping(i["bbb_rating"])
        elif len(query) >= 2:
            logger.info(f"{i['bbb_url']} has updated rating!")
            logger.warning(f"{i['bbb_url']} has {len(query)} duplicate rows in the db")
            for c in query:
                c.bbb_rating = bbb_rating_mapping(i["bbb_rating"])
