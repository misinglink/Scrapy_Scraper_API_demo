from scrapy.item import Item, Field


class ProCorpusItem(Item):
    bbb_url = Field()
    yelp_url = Field()
    pro_url = Field()
    bbb_overview = Field()
    bbb_categories = Field()
    bbb_products_services = Field()
    yelp_services = Field()
    yelp_category = Field()
    pro_text = Field()
    mongoid = Field()

    pro_text_bs4 = Field()
