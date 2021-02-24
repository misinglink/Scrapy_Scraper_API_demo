import os, datetime

# importing module
import logging

import pymongo
from pymongo import MongoClient
import pandas as pd

from func import bbb_rating_mapping, to_dict, session_scope
from db import Base, Company, pro_coll

# Create and configure logger
logging.basicConfig(
    filename="upload_accredited.log", format="%(message)s", filemode="w"
)

logger = logging.getLogger()

# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

accredited_pros = [
    dict(x)
    for x in pro_coll.find(
        {"in_production": True, "is_accredited": True},
        {"_id": False, "bbb_url": True, "is_accredited": True},
    )
]

##5988 unique bbb_urls in production according to mongo db
# check prod in heroku query tool!


for p in accredited_pros:
    with session_scope() as session:
        query = list(
            session.query(Company).filter(Company.bbb_url == p["bbb_url"]).all()
        )

        if len(query) == 0:
            logger.info(f"{p['bbb_url']} is not in prod db or has accreditation data")
        elif len(query) == 1:
            logger.info(f"{p['bbb_url']} has updated accreditation data!")
            company = query[0]
            company.is_accredited = True
        elif len(query) >= 2:
            logger.info(f"{p['bbb_url']} has updated accreditation data!")
            logger.warning(f"{p['bbb_url']} has {len(query)} duplicate rows in the db")
            for c in query:
                c.is_accredited = True
