import os
from urllib.parse import urlparse

import scrapy
from scrapy import Request
from scraper_api import ScraperAPIClient
import pymongo
from dotenv import load_dotenv
from bs4 import BeautifulSoup

from ..items import ProCorpusItem

load_dotenv()

client = ScraperAPIClient(os.environ["SCRAPER_API"])
mongoclient = pymongo.MongoClient()
db = mongoclient[os.environ["mongo_db"]]
pro_coll = db["ProDataCollection"]
corp_coll = db["CorpusCollection"]


class ProCorpusSpider(scrapy.Spider):
    name = "pro_corpus"

    def start_requests(self):

        # link_sets = [
        #     [x["bbb_url"], x["yelp_url"], x["pro_url"]]
        #     if "pro_url" in dict(x)
        #     else [x["bbb_url"], x["yelp_url"]]
        #     for x in pro_coll.find(
        #         {"yelp_url": {"$regex": "^https://www.yelp.com"},
        #         "bbb_url": {"$nin": corp_coll.distinct('bbb_url')}},
        #         {"_id": False, "bbb_url": True, "yelp_url": True, "pro_url": True},
        #     )
        # ]

        link_sets = [
            [x["pro_url"], x["_id"]]
            for x in corp_coll.find(
                {"pro_url": {"$regex": "^http"}, "monogid": {"$exists": False}},
                {"_id": True, "pro_url": True},
            )
        ]
        for s in link_sets:
            print(s)
            item = ProCorpusItem()

            if s[0].startswith("http"):
                item["mongoid"] = s[1]
                item["pro_url"] = s[0]

                yield Request(
                    client.scrapyGet(
                        url=s[0],
                        render=True,
                        country_code="US",
                    ),
                    callback=self.parse_pro_url,
                    cb_kwargs=dict(item=item),
                )
            elif len(s) == 10:
                item["bbb_url"] = s[0]
                item["yelp_url"] = s[1]

                yield Request(
                    client.scrapyGet(
                        url=s[0],
                        render=True,
                        country_code="US",
                    ),
                    callback=self.parse_bbb_url,
                    cb_kwargs=dict(item=item),
                )

            elif len(s) == 3:

                item["bbb_url"] = s[0]
                item["yelp_url"] = s[1]
                item["pro_url"] = s[2]

                yield Request(
                    s[0],
                    callback=self.parse_bbb_url,
                    cb_kwargs=dict(item=item),
                )

    def parse_bbb_url(self, response, item):
        overview_xpath = "//div/div[contains(concat(' ', normalize-space(@class), ' '), ' OverviewParagraph-sc-1k7cf7n-0 ')]/span/span//text()"
        category_xpath = "//div[contains(concat(' ', normalize-space(@class), ' '), ' business-categories-card__content ')]/div//text()"
        products_services_xpath = "//div[contains(concat(' ', normalize-space(@class), ' '), ' dtm-products-services ')]/text()"

        bbb_overview = response.selector.xpath(overview_xpath).extract()
        bbb_categories = response.selector.xpath(category_xpath).extract()
        products_services = response.selector.xpath(products_services_xpath).extract()

        item["bbb_overview"] = "" if bbb_overview == [] else bbb_overview
        item["bbb_categories"] = "" if bbb_categories == [] else bbb_categories
        item["bbb_products_services"] = (
            "" if products_services == [] else products_services
        )

        yield Request(
            urlparse(item["pro_url"], scheme="http").geturl(),
            callback=self.parse_pro_url,
            cb_kwargs=dict(item=item),
        )

    # def parse_yelp_url(self, response, item):
    #     ## get services from yelp website

    #     services_a_xpath = "//section[@aria-label='Services Offered']/div/div//a/text()"
    #     services_p_xpath = "//section[@aria-label='Services Offered']/div/div//p/text()"

    #     services_trip_a = "//section[@aria-label='Services Offered']/div/div/div//a/text()"
    #     services_trip_p = "//section[@aria-label='Services Offered']/div/div/div//p/text()"

    #     yelp_a_services = response.selector.xpath(services_a_xpath).extract()
    #     yelp_p_services = response.selector.xpath(services_p_xpath).extract()
    #     triple_diva_services = response.selector.xpath(services_trip_a).extract()
    #     triple_divp_services = response.selector.xpath(services_trip_p).extract()

    #     yelp_services = yelp_a_services + yelp_p_services + triple_diva_services + triple_divp_services

    #     item['yelp_services'] = '' if yelp_services == [] else yelp_services

    #     yield Request(
    #                 client.scrapyGet(
    #                     url=item['pro_url'],
    #                     render=True,
    #                     country_code="US",
    #                 ),
    #                 callback=self.parse_pro_url,
    #                 cb_kwargs=dict(item=item),
    #             )

    def parse_pro_url(self, response, item):

        body = response.selector.xpath("//body").extract()[0]

        soup = BeautifulSoup(body, "html.parser")
        text = soup.get_text()

        item["pro_text"] = str(text.strip().lower())

        yield item
