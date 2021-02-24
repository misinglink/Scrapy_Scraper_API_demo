import os, sys, datetime, logging
from queue import Queue
from time import time

import pandas as pd

from func import session_scope
from db import Base, Company, pro_coll
from thread_workers import SqlUpdateWorker

log_path = os.path.join("log_files", sys.argv[2])

if not os.path.isdir(log_path):
    f = open(log_path, "w+")
    f.close()

# configure logger
logging.basicConfig(
    filename=log_path,
    format="%(asctime)s - %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S",
    filemode="a",
)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def main():
    ts = time()
    with session_scope() as session:
        bbb_urls = [x[0] for x in session.query(Company.bbb_url).distinct()]

    queue = Queue()

    for i in range(8):
        worker = SqlUpdateWorker(queue)
        worker.daemon = True
        worker.start()

    for url in bbb_urls:
        pro = pro_coll.find_one({"bbb_url": url})
        if pro:
            g_id = pro["google_id"]
            if g_id == "NoPhoneMatch" or g_id == "NoAddressMatch":
                g_id = None
        else:
            logger.warning(f"NOT IN MONGO: {url}")
        queue.put((url, "google_place_id", g_id, logger))

    queue.join()

    print(f"Time: {time() - ts}")


if __name__ == "__main__":
    main()
