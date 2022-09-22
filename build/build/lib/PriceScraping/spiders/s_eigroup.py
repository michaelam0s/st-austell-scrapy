# -*- coding: utf-8 -*-
import re
import io
import json
import time

import scrapy
import random
import datetime

import PriceScraping.spiders.i_helpers as helpers
import PriceScraping.spiders.i_variables as variables

import urllib.parse

from pprint import pprint
from scrapy.http import Request

#
# url=variables.e_url
# allowed_domains=variables.e_allowed_domains
#
# class EiGroupSiteSpider(scrapy.Spider):
#     name='eigroup-sites'
#     site_out=[]
#
#     allowed_domains=allowed_domains
#     start_urls=[url]
#
#     def start_requests(self):
#         for u in self.start_urls:
#            u=u+"Venue/GetVenuesByDistance?longitude={0}&latitude={1}&maxResults={2}".format(-0.0456944383165127,51.5015789519847,9999)
#            yield Request(u,
#                         method="GET",
#                         callback=self.parse)
#
#     def parse(self, response):
#         json_response=json.loads(response.text)
#         venues=json_response['Venues']
#
#         for venue in venues:
#             self.site_out.append(venue)
#             yield venue
#
#
# class EiGroupMenuSpider(scrapy.Spider):
#     name='eigroup-menus'
#     menu_out=[]
#
#     allowed_domains=allowed_domains
#     start_urls=[url]
#
#     def parse(self, response):
#         blob_name=helpers.latest_file('e_sites')
#         blob=variables.blob_service.get_blob_to_bytes(variables.container_name,blob_name).content
#
#         json_sites=json.loads(blob)
#         #json_sites=json_sites[0:1]
#         for site in json_sites:
#             pubName=site['Name']
#             pubCode=site['Id']
#
#             urlS=url+"MenuCategories/GetCategories?venueId={0}".format(pubCode)
#
#             time.sleep(random.randint(1,1))
#             yield Request(urlS,
#                             method="GET",
#                             callback=self.parse_detail,
#                             meta={'pubName':pubName,'pubCode':pubCode})
#
#     def parse_detail(self, response):
#         json_response=json.loads(response.text)
#         menus=json_response['MenuCategories']
#
#         for menu in menus:
#             desc=menu['Name']
#
#             y_keys=['drink']
#             n_keys=['hot','soft']
#             y=re.compile("|".join(y_keys))
#             n=re.compile("|".join(n_keys))
#
#             if y.search(desc.lower()) and not n.search(desc.lower()):
#               menu['pubName']=response.meta['pubName']
#               menu['pubCode']=response.meta['pubCode']
#               self.menu_out.append(menu)
#               yield menu
#
#
# class EiGroupPriceSpider(scrapy.Spider):
#     name='eigroup-prices'
#     price_out=[]
#
#     allowed_domains=allowed_domains
#     start_urls=[url]
#
#     def parse(self, response):
#         blob_name=helpers.latest_file('e_sites')
#         blob=variables.blob_service.get_blob_to_bytes(variables.container_name,blob_name).content
#
#         json_sites=json.loads(blob)
#         #json_sites=json_sites[0:1]
#         for site in json_sites:
#             pubName=site['Name']
#             pubCode=site['Id']
#
#             urlP=url+"Products/GetProducts?venueId={0}".format(pubCode)
#
#             time.sleep(random.randint(1,1))
#             yield Request(urlP,
#                             method="GET",
#                             callback=self.parse_detail,
#                             meta={'pubName':pubName,'pubCode':pubCode})
#
#     def parse_detail(self, response):
#         json_response=json.loads(response.text)
#         prices=json_response['Products']
#
#         for price in prices:
#           prod=price['Name']
#
#           y_keys=variables.products
#           n_keys=['blank']
#           y=re.compile("|".join(y_keys))
#           n=re.compile("|".join(n_keys))
#
#           if y.search(prod.lower()) and not n.search(prod.lower()):
#             price['pubName']=response.meta['pubName']
#             price['pubCode']=response.meta['pubCode']
#             self.price_out.append(price)
#             yield(price)
