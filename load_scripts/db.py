import os, sys

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import pymongo
from dotenv import load_dotenv

load_dotenv()

# connect to SQL db

if sys.argv[1] == "local":
    engine = create_engine(
        f"postgresql+psycopg2://postgres:{os.environ['LOCAL_DB_PASS']}@127.0.0.1:5432/hometool_latest"
    )

elif sys.argv[1] == "staging":
    engine = create_engine(os.environ["STAGING_URI"])

elif sys.argv[1] == "production":
    engine = create_engine(os.environ["PROD_URI"])

Base = automap_base()
Base.prepare(engine, reflect=True)

# create a configured "Session" class
Session = sessionmaker(bind=engine)

# mapped classes
Company = Base.classes["companies_company"]


client = pymongo.MongoClient()
db = client["ProData"]
corp_coll = db["CorpusCollection"]
pro_coll = db["ProCollection"]
