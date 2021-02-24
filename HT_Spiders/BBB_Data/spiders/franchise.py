import os
import logging
from urllib.parse import urljoin
import scrapy
from scrapy import Request
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scraper_api import ScraperAPIClient
import pymongo
from dotenv import load_dotenv

from ..items import CompanyItem

load_dotenv()

client = ScraperAPIClient(os.environ["SCRAPER_API"])
mongoclient = pymongo.MongoClient()
db = mongoclient[os.environ["mongo_db"]]
pro_coll = db["ProDataCollection"]


class FranchiseSpider(CrawlSpider):
    name = "franchise"

    base_url = "https://www.bbb.org/search"
    rules = [
        Rule(
            LinkExtractor(
                allow="profile/",
                restrict_xpaths="//div/div/h3/a",
                deny=[
                    "/details",
                    "/bbb-reports-on",
                    "/overview-of-bbb-ratings",
                    "/accreditation-information",
                ],
            ),
            callback="parse_pro_page",
            follow=True,
        )
    ]
    start_urls = [
        client.scrapyGet(
            f"https://www.bbb.org/search?filter_distance=50&filter_ratings=A&find_country=USA&find_loc={dict(x)['zip_code']}&find_text={dict(x)['company_name']}&page=1&touched=1",
            render=True,
            country_code="US",
        )
        for x in pro_coll.find({"is_franchise": True, "bbb_url": {"$exists": False}})
    ]

    def parse_pro_page(self, response):
        # xpath strings for each component needed
        logo_xpath = "//img[contains(concat(' ',normalize-space(@class),' '),' BusinessLogo__LogoImage-sc-1umu6wn-0 ')]/@src"
        name_xpath = (
            "//header[@class='styles__Title-sc-1nxgmxz-0 bGoUZy']/div/div/h3/text()"
        )
        CONTACT_INFO_XPATH = "//div[@class='MuiCardContent-root Default__CardContent-sc-1b8c5zl-1 eNoSzV']"
        address_xpath = f"{CONTACT_INFO_XPATH}/div/p/text()"
        pro_url_xpath = f"{CONTACT_INFO_XPATH}/div/p/a/@href"
        phone_xpath = f"{CONTACT_INFO_XPATH}/div/a[@class='dtm-phone']/text()"
        user_rating_xpath = "//div/div/div/span/strong/text()"
        bbb_rating_xpath = "//div/div/a/span[contains(concat(' ',normalize-space(@class), ' '), ' LetterGrade-sc-1exw0et-0 ')]/text()"
        acc_xpath = "//a/picture/img/@alt"

        logo = response.selector.xpath(logo_xpath).get()
        logo = "/Terminus" if logo == None else logo
        name = response.selector.xpath(name_xpath).get()
        address = response.selector.xpath(address_xpath).extract()[:6]
        pro_url = response.selector.xpath(pro_url_xpath).get()
        phone = response.selector.xpath(phone_xpath).get()
        user_rating = response.selector.xpath(user_rating_xpath).get()
        bbb_rating = response.selector.xpath(bbb_rating_xpath).get()
        is_accredited = response.selector.xpath(acc_xpath).get()

        item = CompanyItem()

        item["company_name"] = name
        item["logo"] = (
            logo
            if not logo.startswith("/Terminus")
            and (
                logo
                != "https://www.bbb.org/corecmsimages/3eea7efc-0827-4425-a2ee-1992376837fb.png"
            )
            else "No BBB logo"
        )
        item["phone_no"] = phone
        item["bbb_url"] = response.url
        item["pro_url"] = "None" if pro_url == None else pro_url
        item["user_ratings"] = "None" if user_rating == None else user_rating
        item["bbb_rating"] = bbb_rating
        item["is_accredited"] = (
            True if is_accredited == "BBB accredited business" else False
        )

        try:
            item["address_line_1"] = address[0]
            item["city"] = address[1]
            item["zip_code"] = address[-1][:5]
            item["state"] = address[-3]
        except:
            item["address_line_1"] = "None"
            item["city"] = item["bbb_url"].split("/")[5]
            item["zip_code"] = "None"
            item["state"] = item["bbb_url"].split("/")[4]
            self.logger.warning(f"Could not parse address for {item['bbb_url']}")

        if item["pro_url"] != "None":
            yield Request(
                item["pro_url"],
                callback=self.get_pro_url_code,
                cb_kwargs=dict(item=item),
            )
        else:
            yield item

    def get_pro_url_code(self, response, item):
        logos = [
            x
            for x in response.xpath("//img//@src").extract()
            if ("logo" in x and "bbb" not in x and "att/logo.png" not in x)
        ]
        logos = [
            urljoin(item["pro_url"], x)
            if (not x.startswith("https://") or not x.startswith("http://"))
            else x
            for x in logos
        ]
        item["pro_logo"] = "None" if len(logos) == 0 else logos

        yield item
