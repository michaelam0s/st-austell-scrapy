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
logging.getLogger("snowflake.connector.network").disabled = True

Testmode = True
Limit = True


N = 10 if Testmode else 10**8

Date = datetime.today().strftime('%Y-%m-%d')

url=variables.g_url
allowed_domains= variables.g_allowed_domains
s_keep_list = variables.g_s_keep_list
s_alter_list = variables.g_s_alter_list

del_list = variables.g_del_list
alter_list = variables.g_alter_list

site_dtype = variables.site_dtype
site_headers = variables.site_headers

price_headers = variables.price_headers
price_dtype = variables.price_dtype

engine = variables.engine

class GreeneKingSiteSpiderClean(scrapy.Spider):
    name='greeneking-sites-formatted'
    site_out=[]

    allowed_domains=allowed_domains
    start_urls=[url]

    def parse(self, response):
        variables.g_payload['request']['method']="venues"
        variables.g_payload['request']['userDeviceIdentifier']=helpers.user_generator()
        
        site_data=urllib.parse.quote(json.dumps(variables.g_payload)) 
        bdy="request="+site_data

        yield Request(response.url, 
                        method='POST', 
                        body=bdy, 
                        headers=variables.g_headers, 
                        callback=self.parse_detail)

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        venues=json_response['venues']

        for site in venues:
            site['Address Line 1'] = site['address']['line1']
            try:
                NUM = int(site['displayImage'].split("/")[-1].split(".")[0])
                print(NUM)

                if NUM == 2779:
                    site['Format'] = 'Greene King'
                elif NUM == 2775:
                    site['Format'] = 'Farmhouse Inns'
                elif NUM == 93:
                    site['Format'] = 'Hungry Horse'
                elif NUM == 2772:
                    site['Format'] = 'Flaming Grill'
                elif NUM == 5229:
                    site['Format'] = 'L&T'
                elif NUM == 2777:
                    site['Format'] = 'Loch Fyne'
                else:
                    site['Format'] = 'Greene King'
            except:
                site['Format'] = 'Greene King'
            site['Company'] = 'Greene King'
            site['Country'] = site['address']['country']['name']
            site['County'] = site['address']['county']
            site['Date'] = Date
            site['Latitude'] = site['address']['location']['latitude']
            site['Longitude'] = site['address']['location']['longitude']
            site['Other Town'] = site['address']['town']
            site['Postcode'] = site['address']['postcode']
            site['Site Number'] = f'''GK-{site['id']}'''
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

    def closed(self, reason):
        Site_Data = pd.json_normalize(self.site_out)
        Site_Data = Site_Data[site_headers]
        Site_Data = Site_Data.astype(str)
        print(Site_Data.head(50).to_string())
        Site_Data = Site_Data.replace(r'^\s*$', np.nan, regex=True)
        if Site_Data is not None:
            database_Output = 'competitor_sites'
            Site_Data.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000, dtype=site_dtype)
        else:
            print("Empty Dataframe")
            pass

class GreenekingSiteSpider(scrapy.Spider):
    name='greeneking-sites'

    allowed_domains=variables.g_allowed_domains
    start_urls=[variables.g_url]

    def parse(self, response):
        variables.g_payload['request']['method']="venues"
        variables.g_payload['request']['userDeviceIdentifier']=helpers.user_generator()
        site_data=urllib.parse.quote(json.dumps(variables.g_payload))
        bdy="request="+site_data

        yield Request(
            response.url,
            method='POST',
            body=bdy,
            headers=variables.g_headers,
            callback=self.parse_detail
        )

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        sites=json_response['venues'][0:N]

        for site in sites:
          site['siteid']=site['id']
          yield site

class GreeneKingMenuSpider(scrapy.Spider):
    name='greeneking-menus'
    down='greeneking-sites'

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

          variables.g_payload['request']['method']="getmenus"
          variables.g_payload['request']['siteId']=siteId
          variables.g_payload['request']['salesAreaId']=salesAreaId
          
          menu_data=urllib.parse.quote(json.dumps(variables.g_payload)) 
          bdy="request="+menu_data

          time.sleep(random.randint(0,2))
          yield Request(response.url, 
                          method='POST', 
                          body=bdy, 
                          headers=variables.g_headers, 
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
            menu['Format'] = 'Greene King'
            yield menu


class GreeneKingPriceSpider(scrapy.Spider):
    name = 'greeneking-prices-formatted'
    down = 'greeneking-menus'
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

            variables.g_payload['request']['method'] = "getmenupages"
            variables.g_payload['request']['siteId'] = siteId
            variables.g_payload['request']['salesAreaId'] = salesAreaId
            variables.g_payload['request']['menuId'] = menuId

            time.sleep(random.randint(0, 1))

            price_data = urllib.parse.quote(json.dumps(variables.g_payload))
            bdy = "request=" + price_data
            yield Request(
                response.url,
                method='POST',
                body=bdy,
                headers=variables.g_headers,
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
                products['Site Number'] = f'''GK-{response.meta['siteId']}'''
                products['salesAreaId'] = response.meta['salesAreaId']
                products['Menu ID'] = response.meta['menuId']
                products['Menu Name'] = response.meta['Menu Name']
                products['Product Name'] = price['eposName']
                Prod_Name = price['eposName']
                Prod_ID = str(price['eposName']) + str(products['name'])
                products['Product ID'] = hashlib.md5(Prod_ID.encode()).hexdigest()
                products['Date'] = Date
                products['Company'] = 'Greene King'
                products['Format'] = response.meta['Format']
                products = helpers.delete_items(products, del_list)
                products = helpers.rename_items(products, alter_list)
                products = helpers.keep_items(products, price_headers)

                Data.append(products)

                yield products

        self.out = self.out.append(pd.json_normalize(Data))
        if len(self.out.index) >= 100000 and Testmode != True:
                out = self.out[price_headers]
                database_Output = 'competitor_prices'
                out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
                           chunksize=16000,
                           dtype=variables.price_dtype_C)
                self.out = pd.DataFrame()


    def closed(self, reason):
        if self.out is not None and Testmode != True:
            print(self.out.head(10).to_string())
            out = self.out[price_headers]
            database_Output = 'competitor_prices'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
                       chunksize=16000,
                       dtype=variables.price_dtype_C)


class GreeneKingPriceSpider_Test(scrapy.Spider):
    name = 'GT'
    down = 'greeneking-menus'
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

            variables.g_payload['request']['method'] = "getmenupages"
            variables.g_payload['request']['siteId'] = siteId
            variables.g_payload['request']['salesAreaId'] = salesAreaId
            variables.g_payload['request']['menuId'] = menuId

            time.sleep(random.randint(0, 1))

            price_data = urllib.parse.quote(json.dumps(variables.g_payload))
            bdy = "request=" + price_data
            yield Request(
                response.url,
                method='POST',
                body=bdy,
                headers=variables.g_headers,
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
                products['Site Number'] = f'''GK-{response.meta['siteId']}'''
                products['salesAreaId'] = response.meta['salesAreaId']
                products['Menu ID'] = response.meta['menuId']
                products['Menu Name'] = response.meta['Menu Name']
                products['Product Name'] = price['eposName']
                Prod_Name = price['eposName']
                Prod_ID = str(price['eposName']) + str(products['name'])
                products['Product ID'] = hashlib.md5(Prod_ID.encode()).hexdigest()
                products['Date'] = Date
                products['Company'] = 'Greene King'
                products['Format'] = response.meta['Format']
                products = helpers.delete_items(products, del_list)
                products = helpers.rename_items(products, alter_list)
                products = helpers.keep_items(products, price_headers)

                Data.append(products)

                yield products
    #
        self.out = self.out.append(pd.json_normalize(Data))
        if len(self.out.index) >= 100000 and Testmode != True:
                out = self.out[price_headers]
                database_Output = 'Competitor_Pricing_All_Products'
                out.to_sql(name=database_Output, con=engine, if_exists='append', index=False, chunksize=16000,
                           dtype=price_dtype)
                self.out = pd.DataFrame()


    def closed(self, reason):
        if self.out is not None and Testmode != True:
            print(self.out.head(10).to_string())
            out = self.out[price_headers]
            database_Output = 'Competitor_Pricing_All_Products'
            out.to_sql(name=database_Output, con=engine, if_exists='append', index=False, chunksize=16000,
                       dtype=price_dtype)

class GK_MPC_Sites(scrapy.Spider):
    name = "GK_MPC_sites_formatted"

    site_out = []

    def start_requests(self):
        url = "https://api.woosmap.com/stores?key=woos-bc5bdec5-08ff-3db9-9de3-50f0cc740a10&query=type%3A%22OEI%20Pubs%20%26%20Eating%20Inn%20%26%20Goodwins%22"

        yield scrapy.Request(url,
                             method='GET',
                             headers=variables.gk_mps_headers,
                             callback=self.parse_length)

    def parse_length(self, response):
        json_response = json.loads(response.text)

        Request_Length = json_response['pagination']['pageCount']

        for i in range(1, Request_Length):
            url = f"https://api.woosmap.com/stores?key=woos-bc5bdec5-08ff-3db9-9de3-50f0cc740a10&query=type%3A%22OEI%20Pubs%20%26%20Eating%20Inn%20%26%20Goodwins%22&page={i}"

            yield Request(url,
                          method='GET',
                          headers=variables.gk_mps_headers,
                          callback=self.parse_sites)

    def parse_sites(self, response):
        json_response = json.loads(response.text)

        for sites in json_response['features']:
            if sites['properties']['types'][0] == "OEI Pubs & Eating Inn & Goodwins":
                site = {}
                site['Address Line 1'] = sites['properties']['address']['lines'][0]
                site['Company'] = 'Greene King'
                site['Country'] = sites['properties']['address']['country_code']
                site['County'] = sites['properties']['address']['lines'][2]
                site['Date'] = Date
                site['Format'] = sites['properties']['types'][0]
                site['Given ID'] = sites['properties']['store_id']
                site['Latitude'] = sites['geometry']['coordinates'][1]
                site['Longitude'] = sites['geometry']['coordinates'][0]
                site['Other Town'] = sites['properties']['address']['lines'][1]
                site['Postcode'] = sites['properties']['address']['zipcode']
                site['Site Name'] = sites['properties']['name']
                site['Site Number'] = f'''GK-{sites['properties']['store_id']}'''
                site['Status'] = 'OPEN'
                site['Telephone Number'] = sites['properties']['contact']['phone']
                site['Town'] = sites['properties']['address']['lines'][1]

                self.site_out.append(site)
                yield site

    def closed(self, reason):
        Site_Data = pd.json_normalize(self.site_out)
        Site_Data = Site_Data[site_headers]
        Site_Data = Site_Data.astype(str)
        print(Site_Data.head(5).to_string())
        Site_Data = Site_Data.replace(r'^\s*$', np.nan, regex=True)
        if Site_Data is not None:
            database_Output = 'Competitor_Houselist'
            Site_Data.to_sql(name=database_Output, con=engine, if_exists='append', index=False, chunksize=16000, dtype=site_dtype)
        else:
            print("Empty Dataframe")
            pass


class GK_MPC_Prices(scrapy.Spider):
    name = "GK_MPC_prices_formatted"

    out = pd.DataFrame()

    i = 0

    def start_requests(self):
        url = "https://api.woosmap.com/stores?key=woos-bc5bdec5-08ff-3db9-9de3-50f0cc740a10&query=type%3A%22OEI%20Pubs%20%26%20Eating%20Inn%20%26%20Goodwins%22"

        yield scrapy.Request(url,
                             method='GET',
                             headers=variables.gk_mps_headers,
                             callback=self.parse_length)

    def parse_length(self, response):
        json_response = json.loads(response.text)

        Request_Length = json_response['pagination']['pageCount']

        for i in range(1, Request_Length):
            url = f"https://api.woosmap.com/stores?key=woos-bc5bdec5-08ff-3db9-9de3-50f0cc740a10&query=type%3A%22OEI%20Pubs%20%26%20Eating%20Inn%20%26%20Goodwins%22&page={i}"

            yield Request(url,
                          method='GET',
                          headers=variables.gk_mps_headers,
                          callback=self.parse_sites)

    def parse_sites(self, response):
        json_response = json.loads(response.text)

        for sites in json_response['features']:
            if sites['properties']['types'][0] == "OEI Pubs & Eating Inn & Goodwins":

                if "https://" in sites['properties']['contact']['website']:
                    u = sites['properties']['contact']['website']
                else:
                    u = f'''https://{sites['properties']['contact']['website']}'''

                yield Request(u,
                              method='GET',
                              callback=self.website,
                              meta={
                                  "Company": "Greene King",
                                  "Format": f'''{sites['properties']['types'][0]}''',
                                  "Site Name": f'''{sites['properties']['name']}''',
                                  "Site Number": f'''{sites['properties']['store_id']}'''
                              })
                time.sleep(random.randint(1, 4))

    def website(self, response):
        meta = response.meta
        site_url = meta['download_slot']

        if "https://" in site_url:
            u = site_url
        else:
            u = f'''https://{site_url}'''

        url = f'https://{site_url}/menus'

        yield Request(url,
                      method='GET',
                      callback=self.parse_website,
                      meta={'Link': site_url,
                            "Company": "Greene King",
                            "Format": response.meta['Format'],
                            "Site Name": response.meta['Site Name'],
                            "Site Number": response.meta['Site Number']
                            })
        time.sleep(random.randint(1, 4))

    def parse_website(self, response):
        resp = response
        for href in resp.xpath('/html/body/div/div[1]/main/div/div/div[2]/div/div/ul/li/a'):
            if href.xpath('@class').extract()[0] == 'GKMenusNavigation-menuLink':
                print(href.xpath('@href').extract()[0])
                if "https://" in response.meta['Link']:
                    u = response.meta['Link']
                else:
                    u = f'''https://{response.meta['Link']}'''
                url = f'''{u}{href.xpath('@href').extract()[0]}'''

                yield Request(
                    url,
                    method='GET',
                    callback=self.parse_menus,
                    meta={
                        "Link": response.meta['Link'],
                        "Company": "Greene King",
                        "Format": response.meta['Format'],
                        "Site Name": response.meta['Site Name'],
                        "Site Number": response.meta['Site Number']
                    })
                time.sleep(random.randint(1, 4))

    def parse_menus(self, response):

        Data = []

        for href in response.xpath('/html/body/div/div[1]/main/div/div/div[1]/div/div/div/div/div'):
            if href.xpath('@class').extract()[0] == "Box Box--menuSection":
                Menu_Name = href.xpath('div/h2/text()[normalize-space()]').extract()[0]
                for items in href.xpath('div/div/div'):
                    Product = items.xpath('h3//text()[normalize-space()]').extract()[0].strip()
                    Price = items.xpath('div//text()[normalize-space()]').extract()[0].strip()

                    Price = helpers.Price_Select(Price)

                    Products = {}
                    Products['Company'] = response.meta['Company']
                    Products['Date'] = Date
                    Products['Format'] = response.meta['Format']
                    Products['Menu ID'] = hashlib.md5(Menu_Name.encode()).hexdigest()
                    Products['Menu Name'] = Menu_Name
                    Products['Portion Size'] = "Standard"
                    Products['Price'] = Price
                    Products['Product ID'] = hashlib.md5(Product.encode()).hexdigest()
                    Products['Product Name'] = Product
                    Products['Site Name'] = response.meta['Site Name']
                    Products['Site Number'] = f'''BP-{response.meta['Site Number']}'''

                    try:
                        float(Price)
                        Data.append(Products)
                        yield Products

                    except:
                        pass

        try:
            self.out = self.out.append(pd.json_normalize(Data))
        except:
            pass

        if len(self.out.index) >= 100000:
            out = self.out[price_headers]
            database_Output = 'ingestion_competitor_prices'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
                       chunksize=16000,
                       dtype=variables.price_dtype_C)
            self.out = pd.DataFrame()

    def closed(self, reason):
        if self.out is not None:
            print(self.out.head(10).to_string())
            out = self.out[price_headers]
            database_Output = 'ingestion_competitor_prices'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
                       chunksize=16000,
                       dtype=variables.price_dtype_C)




class test(scrapy.Spider):
    name = 't'

    def start_requests(self):
        url = 'https://www.thephoenixec2.co.uk/food-and-drink/menu'

        yield Request(
            url,
            method='GET',
            callback=self.website
        )

    def website(self, response):
        # print(response.xpath('/html/body/div/div[1]/main/div/div/div[1]/div/div/div/div'))
        for href in response.xpath('/html/body/div/div[1]/main/div/div/div[1]/div/div/div/div/div'):
            if href.xpath('@class').extract()[0] == "Box Box--menuSection":
                Menu_Name = href.xpath('div/h2/text()[normalize-space()]').extract()[0]
                for items in href.xpath('div/div/div'):
                    Product = items.xpath('h3//text()[normalize-space()]').extract()[0].strip()
                    Price = items.xpath('div//text()[normalize-space()]').extract()[0].strip()




# class GreeneKingPriceSpiderKeyProducts(scrapy.Spider):
#     name = 'greeneking-prices-formatted-key-products'
#     down = 'greeneking-menus'
#     data_out=[]
#
#     allowed_domains=allowed_domains
#     start_urls=[url]
#
#     def parse(self, response):
#         job = helpers.latest_items(variables.shubkey, variables.projectID, self.down)
#         print(self.down, helpers.metadate_items(job))
#
#         for menu in job.items.iter(count=N):
#             name=menu['name']
#             siteId=menu['siteId']
#             salesAreaId=menu['salesAreaId']
#             menuId=menu['id']
#             menuName=menu['description']
#
#             variables.g_payload['request']['method']="getmenupages"
#             variables.g_payload['request']['siteId']=siteId
#             variables.g_payload['request']['salesAreaId']=salesAreaId
#             variables.g_payload['request']['menuId']=menuId
#
#             time.sleep(random.randint(0,1))
#
#             price_data=urllib.parse.quote(json.dumps(variables.g_payload))
#             bdy="request="+price_data
#             yield Request(response.url,
#                             method='POST',
#                             body=bdy,
#                             headers=variables.g_headers,
#                             callback=self.parse_detail,
#                             meta={'name':name,'siteId':siteId,'salesAreaId':salesAreaId, 'menuId':menuId, 'Menu Name': menuName})
#
#     def parse_detail(self, response):
#         json_response=json.loads(response.text)
#         prices=json_response['aztec']['products']
#
#         for price in prices:
#             prod=price['eposName']
#             for products in price['portions']:
#                 products['Site Name'] = response.meta['name']
#                 products['Site Number'] = f'''GK-{response.meta['siteId']}'''
#                 products['salesAreaId'] = response.meta['salesAreaId']
#                 products['Menu ID'] = response.meta['menuId']
#                 products['Menu Name'] = response.meta['Menu Name']
#                 products['Product Name'] = price['eposName']
#                 Prod_Name = price['eposName']
#                 Prod_ID = str(price['eposName']) + str(products['name'])
#                 products['Product ID'] = hashlib.md5(Prod_ID.encode()).hexdigest()
#                 products['Date'] = Date
#                 products['Company'] = 'Greene King'
#                 products['Format'] = 'Pub'
#                 products = helpers.delete_items(products, del_list)
#                 products = helpers.rename_items(products, alter_list)
#                 products = helpers.keep_items(products, price_headers)
#
#
#                 if str(products['Product ID']) in y:
#                     print(products['Product ID'])
#                     print(products['Product Name'].lower())
#                     products['Stonegate Product'] = helpers.Key_Products_To_Json(Key_Products_Filtered, products['Product ID'])
#                     self.data_out.append(products)
#                     yield products
#
#     def closed(self, reason):
#         if self.data_out:
#             Price_Data = pd.json_normalize(self.data_out)
#             Price_Data = Price_Data[price_headers]
#             print(Price_Data.head(20).to_string())
#             if Price_Data is not None:
#                 database_Output = 'Competitor_Pricing'
#                 Price_Data.to_sql(name=database_Output, con=engine, if_exists='append', index=False, chunksize=16000, dtype=price_dtype)
#             else:
#                 print("Empty Dataframe")
#                 pass

class GKOpeningTimes(scrapy.Spider):
    name = 'GK-Opening-Times'
    down = 'greeneking-sites'

    out = pd.DataFrame()

    def start_requests(self):
        job = helpers.latest_items(variables.shubkey, variables.projectID, self.down)
        print(self.down, helpers.metadate_items(job))

        for site in job.items.iter(count=N):
            variables.g_payload['request']['venueId'] = site['id']
            variables.g_payload['request']['venueId'] = site['id']
            variables.g_payload['request']['method'] = 'opening'

            price_data = urllib.parse.quote(json.dumps(variables.g_payload))
            bdy = "request=" + price_data

            time.sleep(random.randint(0, 2))

            yield Request(url,
                          method='POST',
                          body=bdy,
                          headers=variables.g_headers,
                          callback=self.parse_detail
                          )

    def parse_detail(self, response):
        json_response = json.loads(response.text)

        for day in json_response['openingTimes']:
            OT = {}
            day['Site Number'] =  f'''GK-{json_response['id']}'''
            OT['Site Number'] =  f'''GK-{json_response['id']}'''
            OT['Date'] = Date
            OT['Day'] = day['label']
            OT['Open'] = day['openingTime']
            OT['Close'] = day['closingTime']

            yield OT
            self.out = self.out.append(pd.json_normalize(OT))

    def closed(self, reason):
        if self.out is not None:
            out = self.out[['Site Number', 'Date', 'Day', 'Open', 'Close']]
            out.to_sql(
                name='COMPETITOR_OPENING_TIMES',
                con=variables.engine_C,
                if_exists='append',
                index=False,
                chunksize=16000
            )
