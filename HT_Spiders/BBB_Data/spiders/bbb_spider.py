from urllib.parse import quote
from time import sleep
from csv import DictReader
import os
import random

import scrapy
from scrapy import Request
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

# from scrapy.utils.response import open_in_browser
from scraper_api import ScraperAPIClient

from ..items import CompanyItem
from dotenv import load_dotenv

load_dotenv()

client = ScraperAPIClient(os.environ["SCRAPER_API"])


class BbbSpiderSpider(CrawlSpider):
    name = "bbb_spider"

    def start_requests(self):
        with open("zip_cat_list3.csv") as file:
            dreader = DictReader(file)
            zip_cat = list(dreader)

        for req in zip_cat:
            print("-" * 40)
            print(req["zip_code"])

            # count URLs in the scheduler
            self.state["URL_count"] = self.state.get("URL_count", 0) + 1

            # initiate an item to capture ht_category
            item = CompanyItem()
            item["ht_category"] = req["category"]

            # send query to the bbb
            for i in range(1, 3):
                yield Request(
                    client.scrapyGet(
                        url=f"https://www.bbb.org/search?filter_distance=50&filter_ratings=A&find_country=USA&find_loc={req['zip_code']}&find_text={req['category']}&page={i}&touched=1",
                        render=True,
                        country_code="US",
                    ),
                    callback=self.parse_item,
                    cb_kwargs=dict(item=item),
                )

    def parse_item(self, response, item):

        # grab all company 'items/cards'
        company_divs = response.selector.xpath(
            "//div[contains(concat(' ',normalize-space(@class),' '),' result-item__content')]"
        )

        # extract info from div with get() or extract()
        for company in company_divs:
            ROOT_SELECTOR = (
                "./div/div[@class='Main-sc-1djngy6-0 hTDGTj result-item__main']/"
            )
            name = company.xpath(ROOT_SELECTOR + "div/h3/a//text()").extract()
            bbb_url = company.xpath(ROOT_SELECTOR + "div/h3/a/@href").get().split("u=")
            bbb_rating = company.xpath(
                ROOT_SELECTOR + "div/div/div/span/text()"
            ).extract()
            phone = company.xpath(ROOT_SELECTOR + "div/p/a/text()").get()
            address = company.xpath(
                ROOT_SELECTOR
                + "div/p[contains(concat(' ',normalize-space(@class),' '),' result-item__address')]/.//text()"
            ).extract()
            city_st_zip = (
                address[-1].split(",") if len(address) > 0 else ["None", "None"]
            )
            accredited = (
                True
                if company.xpath(ROOT_SELECTOR + "div/div/div/img") != []
                else False
            )
            logo = str(
                company.xpath(
                    "./div/div/div[@class='Logo__Link-sc-1w8yjp1-1 bdKudD']/img/@src"
                ).get()
            )

            # try to clean extracted data before inputting to csv
            try:
                item["name"] = "".join(name)
                item["bbb_url"] = bbb_url[1] if len(bbb_url) == 2 else bbb_url[0]
                item["bbb_rating"] = (
                    "Not Rated" if len(bbb_rating) == 0 else bbb_rating[0]
                )
                item["phone"] = phone.strip() if phone else "None"
                item["st_address"] = address[0] if len(address) > 0 else "None"
                item["city"] = city_st_zip[0].strip()

                item["bbb_category"] = item["bbb_url"].split("/")[-2]

                item["state"] = (
                    "None" if city_st_zip[1] == "None" else city_st_zip[1].split(" ")[1]
                )
                item["zip_code"] = (
                    "None" if city_st_zip[1] == "None" else city_st_zip[1].split(" ")[2]
                )
                item["is_accredited"] = accredited

                # make sure  logo url isn't the 'no-logo' logo
                item["logo"] = (
                    logo
                    if not logo.startswith("/Terminus")
                    and (
                        logo
                        != "https://www.bbb.org/corecmsimages/3eea7efc-0827-4425-a2ee-1992376837fb.png"
                    )
                    else "None"
                )

                # visit bbb url of pro to extract website and user ratings
                if str(item["bbb_url"]).startswith("https://www.bbb.org"):
                    yield Request(
                        client.scrapyGet(
                            url=item["bbb_url"], render=True, country_code="US"
                        ),
                        callback=self.parse_pro,
                        cb_kwargs=dict(item=item),
                    )

                else:
                    yield item
            except:
                pass

    def parse_pro(self, response, item):
        # grab the link and user ratings
        pro_url = response.xpath("//div/div/div[3]/p/a//text()").get()
        user_ratings = response.xpath("//div/span/strong//text()").get()

        # populate our item instance
        item["pro_url"] = pro_url
        item["user_ratings"] = user_ratings

        if item["state"] == "None":
            item["state"] = response.url.split("/")[4]
        if item["city"] == "None":
            item["city"] == response.url.split("/")[5]

        # if the pro shows a URL go request logo and/or favicon
        if (item["logo"] == "None") and (pro_url != None):
            yield Request(
                url=pro_url,
                callback=self.get_favicon_and_logo,
                cb_kwargs=dict(item=item),
            )
        else:

            yield item

    def get_favicon_and_logo(self, response, item):

        # pick top favicon result and top logo result and finsih the crawl
        favicon = [
            x for x in response.xpath("//link//@href").extract() if "favicon" in x
        ]
        logo = [x for x in response.xpath("//img//@src").extract() if "logo" in x]

        item["favicon"] = "None" if len(favicon) == 0 else favicon[0]
        item["pro_logo"] = "None" if len(logo) == 0 else logo[0]

        yield item
