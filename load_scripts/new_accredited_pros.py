import os, datetime, sys
import logging

import pymongo
from pymongo import MongoClient
import pandas as pd

from func import (
    bbb_rating_mapping,
    session_scope,
    industry_match,
    slug_logic,
    clean_phone,
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
CompanyIndustry = Base.classes["companies_companyservicetype"]

## connect to pymongo
client = MongoClient()
db = client["ProData"]
pro_coll = db["ProDataCollection"]

# query the pro that are being loaded
new_acc_pros = [
    dict(x)
    for x in pro_coll.find(
        {
            "in_production": True,  #### check the status of in_production flag before running
            "company_name": {"$nin": ["None", "NoMatch", "nan", float("nan"), None]},
            "is_accredited": True,
            "yelp_id": {"$nin": ["None", "NoMatch", "NoPhoneMatch", None]},
        },
        {
            "_id": True,
            "company_name": True,
            "logo": True,
            "bbb_url": True,
            "phone_no": True,
            "address_line_1": True,
            "city": True,
            "state": True,
            "yelp_url": True,
            "pro_url": True,
            "zip_code": True,
            "bbb_rating": True,
            "is_accredited": True,
            "timezone": True,
            "pred_industry": True,
        },
    )
]

# loop through list of pro to upload
for pro in new_acc_pros:

    # create instance of company table
    pro2db = Company(
        name=pro["company_name"],
        bbb_url=pro["bbb_url"],
        logo=pro["logo"],
        website=pro["pro_url"],
        phone_no=clean_phone(pro["phone_no"]) if pro["phone_no"] != None else "",
        address_line_1=pro["address_line_1"],
        city=pro["city"],
        state=pro["state"],
        yelp_url=pro["yelp_url"],
        zip_code=pro["zip_code"],
        bbb_rating=bbb_rating_mapping(pro["bbb_rating"]),
        is_accredited=pro["is_accredited"],
        created_at=datetime.datetime.now(),
        updated_at=datetime.datetime.now(),
        email="",
        address_line_2="",
        subdomain=slug_logic(pro["company_name"]),
        slug=slug_logic(pro["company_name"]),
        facebook_url="",
        twitter_url="",
        googleplus_url="",
        overview="",
        is_insured=False,
        is_bonded=False,
        step_completed=0,
        is_active=False,
        trial_expire_reminder_status=0,
        minimum_appointment_time_window=60,
        minimum_travel_time_window=30,
        yelp_rating_img_url="",
        is_fake=True,
        is_deleted=False,
        timezone=pro["timezone"],
        hide_from=1,
        works_on_crew_level=False,
    )

    # create an instance of the company's industry
    pro2db_industry = CompanyIndustry(
        company_id="",
        free_consultation=False,
        trip_fee_waived=False,
        industry_id=industry_match(pro["pred_industry"]),
        created_at=datetime.datetime.now(),
    )

    # create session to upload pro then close session
    with session_scope(logger) as session:
        # insert pro
        session.add(pro2db)

        # find the newly inserted company id
        new_id = (
            session.query(Company.id).filter(Company.bbb_url == pro["bbb_url"]).scalar()
        )

        # log info pairing mongo id and sql ID
        logger.debug(
            f"company name, city: {pro['company_name']}, {pro['city']} \n"
            + f"pro_mongo_id : {pro['_id']} \n"
            + f"sql_id: {new_id}"
        )

        pro2db_industry.company_id = new_id

        ## relate the predicted industry with the pro
        session.add(pro2db_industry)
