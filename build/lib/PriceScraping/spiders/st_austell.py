# -*- coding: utf-8 -*-
import re
import io
import json
import time
import numpy as np

import scrapy
import random
from datetime import datetime
import hashlib
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from scrapy import linkextractors

import PriceScraping.spiders.i_helpers as helpers
import PriceScraping.spiders.i_variables as variables

import urllib.parse

from pprint import pprint
from scrapy.http import Request
import logging
# logging.getLogger("snowflake.connector.network").disabled = True

Testmode = False
Limit = True


N = 10 if Testmode else 10**8

Date = datetime.today().strftime('%Y-%m-%d')

url=variables.sta_url
allowed_domains= variables.sta_allowed_domains
s_keep_list = variables.sta_s_keep_list
s_alter_list = variables.sta_s_alter_list

del_list = variables.sta_del_list
alter_list = variables.sta_alter_list

site_dtype = variables.site_dtype
site_headers = variables.site_headers

price_headers = variables.price_headers
price_dtype = variables.price_dtype

# engine = variables.engine

class StAustellSiteSpiderClean(scrapy.Spider):
    name='staustell-sites-formatted'
    site_out=[]

    allowed_domains=allowed_domains
    start_urls=[url]

    def parse(self, response):
        variables.sta_payload['request']['method']="venues"
        variables.sta_payload['request']['userDeviceIdentifier']=helpers.user_generator()
        
        site_data=urllib.parse.quote(json.dumps(variables.sta_payload)) 
        bdy="request="+site_data

        yield Request(response.url, 
                        method='POST',
                        body = bdy,
                        # body="request=%7B%22request%22%3A%7B%22username%22%3A%22staustell_wla%22%2C%22password%22%3A%22xzvvtIHR%22%2C%22version%22%3A%221.4.0%22%2C%22bundleIdentifier%22%3A%22uk.co.staustellbrewery.iorder%22%2C%22platform%22%3A%22Android%22%2C%22userDeviceIdentifier%22%3A%22a75be58f-7f0f-4407-9be6-8cbad086d5eb%22%2C%22method%22%3A%22venues%22%7D%7D", 
                        headers=variables.sta_headers, 
                        callback=self.parse_detail)

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        print(json_response)
        venues=json_response['venues']

        for site in venues:
            site['Address Line 1'] = site['address']['line1']
            # try:
            #     NUM = int(site['displayImage'].split("/")[-1].split(".")[0])
            #     # print(NUM)

            #     if NUM == 2779:
            #         site['Format'] = 'St Austell'
            #     elif NUM == 2775:
            #         site['Format'] = 'Farmhouse Inns'
            #     elif NUM == 93:
            #         site['Format'] = 'Hungry Horse'
            #     elif NUM == 2772:
            #         site['Format'] = 'Flaming Grill'
            #     elif NUM == 5229:
            #         site['Format'] = 'L&T'
            #     elif NUM == 2777:
            #         site['Format'] = 'Loch Fyne'
            #     else:
            #         site['Format'] = 'St Austell'
            # except:
            #     site['Format'] = 'St Austell'
            
            site['Format'] = 'St Austell'
            site['Company'] = 'St Austell'
            site['Country'] = site['address']['country']['name']
            site['County'] = site['address']['county']
            site['Date'] = Date
            site['Latitude'] = site['address']['location']['latitude']
            site['Longitude'] = site['address']['location']['longitude']
            site['Other Town'] = site['address']['town']
            site['Postcode'] = site['address']['postcode']
            site['Site Number'] = f'''StAustell-{site['id']}'''
            site['Status'] = 'OPEN'
            if site['comingSoon'] == '1':
                site['Status'] = 'CLOSED'
            site['Given ID'] = site['id']
            site['Site Name'] = site['name']
            site['Telephone Number'] = site['contactDetails']['telephone']
            site['Town'] = site['address']['town']

            site = helpers.keep_items(site, s_keep_list)

            self.site_out.append(site)
            yield site

    # def closed(self, reason):
    #     Site_Data = pd.json_normalize(self.site_out)
    #     Site_Data = Site_Data[site_headers]
    #     Site_Data = Site_Data.astype(str)
    #     print(Site_Data.head(50).to_string())
    #     Site_Data = Site_Data.replace(r'^\s*$', np.nan, regex=True)
    #     if Site_Data is not None:
    #         database_Output = 'competitor_sites'
    #         Site_Data.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000, dtype=site_dtype)
    #     else:
    #         print("Empty Dataframe")
    #         pass

class StAustellSiteSpider(scrapy.Spider):
    name='staustell-sites'

    allowed_domains=variables.sta_allowed_domains
    start_urls=[variables.sta_url]

    def parse(self, response):
        variables.sta_payload['request']['method']="venues"
        variables.sta_payload['request']['userDeviceIdentifier']=helpers.user_generator()
        site_data=urllib.parse.quote(json.dumps(variables.sta_payload))
        bdy="request="+site_data

        yield Request(
            response.url,
            method='POST',
            body=bdy,
            headers=variables.sta_headers,
            callback=self.parse_detail
        )

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        sites=json_response['venues'][0:N]

        for site in sites:
          site['siteid']=site['id']
          yield site

class StAustellMenuSpider(scrapy.Spider):
    name='staustell-menus'
    down='staustell-sites'

    allowed_domains=allowed_domains
    start_urls=[url]

    def parse(self, response):
        job = helpers.latest_items(variables.shubkey, variables.projectID, self.down)
        print(self.down, helpers.metadate_items(job))

        for site in job.items.iter(count=N):
          name=site['name']
          siteId=site['id']


          try:
            salesAreaId=site['salesArea'][0]['id'] #bug fix
          except:
            salesAreaId=0

          variables.sta_payload['request']['method']="getmenus"
          variables.sta_payload['request']['siteId']=siteId
          variables.sta_payload['request']['salesAreaId']=salesAreaId
          
          menu_data=urllib.parse.quote(json.dumps(variables.sta_payload)) 
          bdy="request="+menu_data

          time.sleep(random.randint(0,2))
          yield Request(response.url, 
                          method='POST', 
                          body=bdy, 
                          headers=variables.sta_headers, 
                          callback=self.parse_detail,
                          meta={'name':name,'siteId':siteId,'salesAreaId':salesAreaId})

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        menus=json_response['menus']

        for menu in menus:
            desc=menu['name']

            y_keys=['drink']
            n_keys=['hot','soft']
            y=re.compile("|".join(y_keys))
            n=re.compile("|".join(n_keys))

            # if y.search(desc.lower()) and not n.search(desc.lower()):
            menu['description']=desc
            menu['name']=response.meta['name']
            menu['siteId']=response.meta['siteId']
            menu['salesAreaId']=response.meta['salesAreaId']
            menu['Format'] = 'St Austell'
            yield menu


class StAustellPriceSpider(scrapy.Spider):
    name = 'staustell-prices-formatted'
    down = 'staustell-menus'
    out = pd.DataFrame()

    allowed_domains = allowed_domains
    start_urls = [url]

    def parse(self, response):
        job = helpers.latest_items(variables.shubkey, variables.projectID, self.down)
        print(self.down, helpers.metadate_items(job))

        for menu in job.items.iter(count=N):
            name = menu['name']
            siteId = menu['siteId']
            salesAreaId = menu['salesAreaId']
            menuId = menu['id']
            menuName = menu['description']

            variables.sta_payload['request']['method'] = "getmenupages"
            variables.sta_payload['request']['siteId'] = siteId
            variables.sta_payload['request']['salesAreaId'] = salesAreaId
            variables.sta_payload['request']['menuId'] = menuId

            time.sleep(random.randint(0, 1))

            price_data = urllib.parse.quote(json.dumps(variables.sta_payload))
            bdy = "request=" + price_data
            yield Request(
                response.url,
                method='POST',
                body=bdy,
                headers=variables.sta_headers,
                callback=self.parse_detail,
                meta={'name': name, 'siteId': siteId, 'salesAreaId': salesAreaId, 'menuId': menuId,
                      'Menu Name': menuName, 'Format': menu['Format']}
            )

    def parse_detail(self, response):
        json_response = json.loads(response.text)
        prices = json_response['aztec']['products']

        Data = []
        for price in prices:
            prod = price['eposName']
            for products in price['portions']:
                products['Site Name'] = response.meta['name']
                products['Site Number'] = f'''StAustell-{response.meta['siteId']}'''
                products['salesAreaId'] = response.meta['salesAreaId']
                products['Menu ID'] = response.meta['menuId']
                products['Menu Name'] = response.meta['Menu Name']
                products['Product Name'] = price['eposName']
                Prod_Name = price['eposName']
                Prod_ID = str(price['eposName']) + str(products['name'])
                products['Product ID'] = hashlib.md5(Prod_ID.encode()).hexdigest()
                products['Date'] = Date
                products['Company'] = 'St Austell'
                products['Format'] = response.meta['Format']
                products = helpers.delete_items(products, del_list)
                products = helpers.rename_items(products, alter_list)
                products = helpers.keep_items(products, price_headers)

                Data.append(products)

                yield products

        self.out = self.out.append(pd.json_normalize(Data))
        # if len(self.out.index) >= 100000 and Testmode != True:
        #         out = self.out[price_headers]
        #         database_Output = 'competitor_prices'
        #         out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
        #                    chunksize=16000,
        #                    dtype=variables.price_dtype_C)
        #         self.out = pd.DataFrame()


    # def closed(self, reason):
    #     if self.out is not None and Testmode != True:
    #         print(self.out.head(10).to_string())
    #         out = self.out[price_headers]
    #         database_Output = 'competitor_prices'
    #         out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
    #                    chunksize=16000,
    #                    dtype=variables.price_dtype_C)
