# # -*- coding: utf-8 -*-
# import re
# import io
# import json
# import time
#
# import scrapy
# import random
# import datetime
#
# import PriceScraping.spiders.i_helpers as helpers
# import PriceScraping.spiders.i_variables as variables
#
# import urllib.parse
#
# from pprint import pprint
# from scrapy.http import Request
#
# s_url=variables.wp_urlS
# d_url=variables.wp_urlD
# allowed_domains=variables.wp_allowed_domains
#
# Testmode = False
# N = 10 if Testmode else 10**8
#
# class WhatpubSiteSpider(scrapy.Spider):
#     name='whatpub-sites'
#     site_out=[]
#
#     allowed_domains=allowed_domains
#     start_urls=[s_url]
#
#     def start_requests(self):
#         site_data=urllib.parse.urlencode(variables.wp_payload)
#         bdy=site_data
#
#         for u in self.start_urls:
#             yield Request(u,
#                             method="POST",
#                             body=bdy,
#                             headers=variables.wp_headers,
#                             callback=self.parse_detail)
#
#     def parse_detail(self, response):
#         json_response=json.loads(response.text)
#         venues=json_response['response']['Pub']
#
#         for venue in venues:
#             self.site_out.append(venue)
#             yield venue
#
# class WhatpubMenuSpider(scrapy.Spider):
#     name='whatpub-details'
#     down = 'whatpub-sites'
#     menu_out=[]
#
#     allowed_domains=allowed_domains
#     start_urls=[d_url]
#
#     def start_requests(self):
#         job = helpers.latest_items(variables.shubkey, variables.projectID, self.down)
#         print(self.down, helpers.metadate_items(job))
#
#         for u in self.start_urls:
#
#             for site in job.items.iter(count=N):
#                 siteId=site['PubID']
#
#                 variables.wp_payload['PubID']=siteId
#
#                 menu_data=urllib.parse.urlencode(variables.wp_payload)
#                 bdy=menu_data
#
#                 #time.sleep(random.randint(1,2))
#                 yield Request(u,
#                                 method="POST",
#                                 body=bdy,
#                                 headers=variables.wp_headers,
#                                 callback=self.parse_detail)
#
#     def parse_detail(self, response):
#         json_response=json.loads(response.text)
#         details=json_response['response']
#
#         # self.menu_out.append(details)
#         yield details

