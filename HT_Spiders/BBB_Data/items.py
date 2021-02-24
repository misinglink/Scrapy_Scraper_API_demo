# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html


from scrapy.item import Item, Field


class CompanyItem(Item):
    company_name = Field()
    bbb_url = Field()
    bbb_rating = Field()
    bbb_category = Field()
    logo = Field()
    phone_no = Field()
    address_line_1 = Field()
    city = Field()
    state = Field()
    zip_code = Field()
    is_accredited = Field()
    pro_url = Field()
    user_ratings = Field()
    pro_logo = Field()
    ## added for drop duplicate spider functionality
    # in_production = Field()
    # yelp_url = Field()
    # ht_category = Field()
    # industry = Field()
    # is_franchise = Field()
    # pred_industry = Field()
    # service_area = Field()
    # website = Field()
    # address_line_2 = Field()
    # name = Field()


# class MongoPro(Item):
#     mongoid = Field()
#     name = Field()
#     bbb_url = Field()
#     logo = Field()
#     phone = Field()
#     st_address = Field()
#     is_accredited = Field() ## get from page
#     city = Field()
#     state = Field()
#     zip_code = Field()
#     pro_url = Field()
#     user_ratings = Field()
#     bbb_rating = Field()
#     pro_url_code = Field()
