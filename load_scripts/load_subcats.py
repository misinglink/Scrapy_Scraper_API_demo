import os, sys, re, datetime
import logging

import pymongo
from pymongo import MongoClient
import pandas as pd
from bson.objectid import ObjectId

from func import (
    bbb_rating_mapping,
    session_scope,
    industry_match,
    slug_logic,
    clean_phone,
    to_dict,
    clean_str,
    clean_special,
)
from db import Base

# Create and configure logger
logging.basicConfig(
    filename=os.path.join("log_files", sys.argv[2]),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
    filemode="w",
)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# map company table
Company = Base.classes["companies_company"]
Service = Base.classes["commons_servicetype"]
CompanyService = Base.classes["companies_companyservicetype_service_type"]

## connect to pymongo
client = MongoClient()
db = client["ProData"]
pro_coll = db["ProDataCollection"]


# create conversion to dict to match my alpha version of the service name to the
# service_id in the db
with session_scope(logger) as session:
    services = [x._asdict() for x in session.query(Service.name, Service.id)]

service_dict = {clean_special(clean_str(x["name"])): x["id"] for x in services}

sqlre = re.compile("(sql_id:)\s\d\d\d\d\d")
mongore = re.compile("(pro_mongo_id\s:)\s\w*")
namere = re.compile("('name':)\s'(.*?)'")

uploaded_ids = []
names = []
with open("./log_files/staging_accredited_load.log") as file:
    for row in file.readlines():
        if mongore.findall(row):
            uploaded_ids.append(row.split(":")[1].strip())

        elif namere.findall(row):
            names.append(namere.findall(row)[0][1])

for idd in uploaded_ids:

    pro = pro_coll.find_one(
        {"_id": ObjectId(idd)},
        {"_id": False, "bbb_url": True, "subcats": True, "company_name": True},
    )

    # create session to upload pro then close session
    with session_scope(logger, "update") as session:
        pro_id = (
            session.query(Company.id).filter(Company.bbb_url == pro["bbb_url"]).one()[0]
        )

        for sub in pro["subcats"]:

            comp_service = CompanyService(
                companyservicetype_id=pro_id, servicetype_id=service_dict[sub]
            )
            logger.debug(f"Adding {sub} to {pro['company_name']}")

            session.add(comp_service)


for name in names:

    pro = pro_coll.find_one(
        {"company_name": name},
        {"_id": False, "bbb_url": True, "subcats": True, "company_name": True},
    )

    # create session to upload pro then close session
    with session_scope(logger, "update") as session:
        pro_id = (
            session.query(Company.id).filter(Company.bbb_url == pro["bbb_url"]).all()
        )

        for sub in pro["subcats"]:

            comp_service = CompanyService(
                companyservicetype_id=pro_id, servicetype_id=service_dict[sub]
            )
            logger.debug(f"Adding {sub} to {pro['company_name']}")

            session.add(comp_service)
