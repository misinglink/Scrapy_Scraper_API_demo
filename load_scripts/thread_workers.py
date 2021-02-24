import os, sys, logging
from threading import Thread

from db import Base, pro_coll, Company
from func import session_scope


class MongoUpdateWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            try:
                url, column, value = self.queue.get()
                with session_scope() as session:

                    _id = (
                        session.query(Company.id)
                        .filter(Company.bbb_url == url)
                        .scalar()
                    )
                print(pro_id)
                pro_coll.update_one(
                    {"bbb_url": url}, {"$set": {f"{sys.argv[1]}_sql_id": pro_id}}
                )
            finally:
                self.queue.task_done()


class SqlUpdateWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            try:
                url, column, value, logger = self.queue.get()

                with session_scope(logger=logger, mode="update") as session:
                    query = list(session.query(Company).filter(Company.bbb_url == url))
                    if len(query) == 0:
                        logger.info(f"{url} is not in prod db")
                    elif len(query) == 1:
                        company = query[0]
                        logger.info(f"ID: {company.id} has updated {column}")
                        session.query(Company).filter(Company.id == company.id).update(
                            {column: value}
                        )
                    elif len(query) >= 2:
                        for c in query:
                            logger.info(f"ID: {c.id} has updated {column}")
                            session.query(Company).filter(Company.id == c.id).update(
                                {column: value}
                            )
            finally:
                self.queue.task_done()
