# python imports
import os, glob, datetime, sys
from random import randint
from time import sleep
import logging

# third party imports
import pandas as pd
from dotenv import load_dotenv

# local imports
from func import (
    to_dict,
    bbb_rating_mapping,
    session_scope,
    industry_match,
    slug_logic,
    clean_phone,
    clear,
    merge_pro,
)
from db import Base, Session, pro_coll

load_dotenv()

Company = Base.classes["companies_company"]

# logger settings
mergelogfile = os.path.join("log_files", sys.argv[2])
mlogger = logging.getLogger()
fhandler = logging.FileHandler(filename=mergelogfile, mode="a")
formatter = logging.Formatter("%(asctime)s - MERGING - %(message)s")
fhandler.setFormatter(formatter)
mlogger.addHandler(fhandler)
mlogger.setLevel(logging.WARNING)

# query current pros
q_session = Session()
Company_df = pd.DataFrame([to_dict(x) for x in q_session.query(Company)])
q_session.close()

## find duplicate bbb_urls
dup_df = Company_df[Company_df.duplicated(keep=False, subset=["yelp_url"])]
dup_df = dup_df[dup_df["yelp_url"] != ""]
columns = list(dup_df.columns)
print(len(dup_df))
# pop columns that wont change with the merge
#### confirm with KIKO
columns.pop(columns.index("id"))
columns.pop(columns.index("created_at"))
columns.pop(columns.index("updated_at"))

# iterate through table and merge the pros to  one row
# and delete the rest of the rows
count = 0

for i, r in dup_df.iterrows():
    print(f"URLs merged: {count}")
    dlicates = dup_df[dup_df["yelp_url"] == r["yelp_url"]]

    p_ind = list(dlicates.index)
    sql_ids = list(dlicates["id"])
    log_message = f"{sql_ids}\n"

    keep_id = int(
        (
            dlicates.loc[p_ind[0], "id"]
            if dlicates.loc[p_ind[0], "id"] < dlicates.loc[p_ind[1], "id"]
            else dlicates.loc[p_ind[1], "id"]
        )
    )
    keep_p_ind = dlicates[dlicates["id"] == keep_id].index[0]

    log_message = (
        log_message + f"\tKeeping: {keep_id}: {dlicates.loc[keep_p_ind, 'name']}\n"
    )

    try:
        print(f"Keeping: {keep_id}: {dlicates.loc[keep_p_ind, 'name']}")
        print(r['bbb_url'])
        log_message = merge_pro(log_message, columns, dlicates, p_ind, keep_id, sql_ids)
        if 'skip' in log_message:
            print('Skipping ... ')
            continue
    except Exception as exp:
        print(exp)
        print("Try Again... \n\n")
        try:
            log_message = merge_pro(log_message, columns, dlicates, p_ind, keep_id, sql_ids)
        except:
            raise
    finally:
        if 'skip' in log_message:
            continue
        try_again = input("need another try?")
        if try_again == "y":
            try:
                log_message = merge_pro(
                    log_message, columns, dlicates, p_ind, keep_id, sql_ids
                )
            except:
                raise
        else:
            pass

        ## pop keep id before using sql_ids to delete duplicates
        _ = sql_ids.pop(sql_ids.index(keep_id))

        log_message = log_message + f"\tDELETING IDS - {sql_ids}"
        # mlogger.warning(log_message)

        ### delete pros not used
        with session_scope(logger=mlogger) as session:
            for i in sql_ids:
                log_message = log_message + f"\tDELETING - {i}"
                del_pro = session.query(Company).filter(Company.id == i).first()
                session.delete(del_pro)

        # log operation and clear the terminal
        mlogger.warning(log_message)
        clear()

    count += 1
