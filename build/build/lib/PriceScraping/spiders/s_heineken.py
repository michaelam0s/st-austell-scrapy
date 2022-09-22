# -*- coding: utf-8 -*-
import re
import io
import json
import time
import pandas as pd
import scrapy
import random
import datetime
from datetime import datetime
import hashlib
import numpy as np

import PriceScraping.spiders.i_helpers as helpers
import PriceScraping.spiders.i_variables as variables

import urllib.parse

from pprint import pprint
from scrapy.http import Request
import logging
logging.getLogger("snowflake.connector.network").disabled = True

url=variables.h_url
allowed_domains=variables.h_allowed_domains

Date = datetime.today().strftime('%Y-%m-%d')

Testmode = False
N = 10 if Testmode else 10**8

class HeinekenSiteSpider(scrapy.Spider):
    name='heineken-sites'
    site_out=[]

    allowed_domains=allowed_domains
    start_urls=[url]

    def start_requests(self):
        for u in self.start_urls:
            u=u+"venues?latitude={0}&longitude={1}&limit={2}".format(52.72759,1.6252,9999)
            yield Request(u,
                            method="GET",
                            headers=variables.r_headers,
                            callback=self.parse_detail)

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        venues=json_response['data']['venues']

        for venue in venues:
            yield venue

class HeinekenSiteSpiderFormatted(scrapy.Spider):
    name='heineken-sites-formatted'
    site_out=[]

    allowed_domains=allowed_domains
    start_urls=[url]

    def start_requests(self):
        for u in self.start_urls:
            u=u+"venues?latitude={0}&longitude={1}&limit={2}".format(52.72759,1.6252,9999)
            yield Request(u,
                            method="GET",
                            headers=variables.r_headers,
                            callback=self.parse_detail)

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        venues=json_response['data']['venues']

        for venue in venues:


            yield venue

class HeinekenMenuSpider(scrapy.Spider):
    name='heineken-menus'
    down = 'heineken-sites'
    menu_out=[]

    allowed_domains=allowed_domains
    start_urls=[url]

    def parse(self, response):
        job = helpers.latest_items(variables.shubkey, variables.projectID, self.down)
        print(self.down, helpers.metadate_items(job))
        
        for site in job.items.iter(count=N):
            pubName=site['title']
            guid=site['guid']
            urlH=url+"menus/{0}".format(guid)
            
            time.sleep(random.randint(1,1))
            yield Request(urlH,
                            method="GET",
                            callback=self.parse_detail,
                            meta={'pubName':pubName,'guid':guid})

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        try:
            menus=json_response['data']['menuitems']
        except:
            menus={}

        if menus:
            for menu in menus:
                desc=menu['section']

                y_keys=['drink',"bottles",'cans','draught']
                n_keys=['hot','soft']
                y=re.compile("|".join(y_keys))
                n=re.compile("|".join(n_keys))

                if y.search(desc.lower()) and not n.search(desc.lower()):
                    for items in menu['menuitemvars']:

                        items['Date'] = Date
                        items['Menu Name']=desc
                        items['pubName']=response.meta['pubName']
                        items['guid']=response.meta['guid']
                        items['Product Name'] = menu['title']
                        prod_id = str(items['Product Name']) + str(items['title'])
                        items['Product ID'] = hashlib.md5(prod_id.encode()).hexdigest()
                        items['Menu ID'] = hashlib.md5(desc.encode()).hexdigest()
                        del items['optiongroups']
                        yield items

class HeinekenPriceSpider(scrapy.Spider):
    name='heineken-prices'
    down = 'heineken-menus'

    allowed_domains=allowed_domains
    start_urls=[url]

    def parse(self, response):
        job = helpers.latest_items(variables.shubkey, variables.projectID, self.down)
        print(self.down, helpers.metadate_items(job))

        for menu in job.items.iter(count=N):
            prod = menu['title']

            y_keys=variables.products
            n_keys=['default']
            y=re.compile("|".join(y_keys))
            n=re.compile("|".join(n_keys))

            # if y.search(prod.lower()) and not n.search(prod.lower()):
            yield(menu)
