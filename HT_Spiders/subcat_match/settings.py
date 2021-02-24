from dotenv import load_dotenv

load_dotenv()

BOT_NAME = "HT-SC-S"

SPIDER_MODULES = ["subcat_match.spiders"]
NEWSPIDER_MODULE = "subcat_match.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False
DOWNLOAD_DELAY = 1

FAKEUSERAGENT_PROVIDERS = [
    "scrapy_fake_useragent.providers.FakeUserAgentProvider",
    "scrapy_fake_useragent.providers.FakerProvider",
]

# Disable cookies (enabled by default)
COOKIES_ENABLED = False
SCHEDULER_DEBUG = True

# LOG_FILE = 'db_data_fill.txt'
# LOG_LEVEL = 'INFO'

DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
    "scrapy_fake_useragent.middleware.RandomUserAgentMiddleware": 400,
    "scrapy_fake_useragent.middleware.RetryUserAgentMiddleware": 401,
}

ITEM_PIPELINES = {
    "subcat_match.pipelines.MongoPipeline": 500,
}
