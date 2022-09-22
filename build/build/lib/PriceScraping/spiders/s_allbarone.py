# -*- coding: utf-8 -*-
import re
import io
import json
import time
import hashlib
import numpy as np
import scrapy
import random
import datetime
import pandas as pd

import PriceScraping.spiders.i_helpers as helpers
import PriceScraping.spiders.i_variables as variables

import urllib.parse

from pprint import pprint
from scrapy.http import Request
import logging
logging.getLogger("snowflake.connector.network").disabled = True


url=variables.a_url
allowed_domains=variables.a_allowed_domains

del_p_list = variables.mb_del_list
alter_p_list = variables.mb_alter_list
keep_s_list = variables.mb_s_keep_list
del_s_list = variables.mb_del_list
alter_s_list = variables.mb_s_alter_list

site_dtype = variables.site_dtype
site_headers = variables.site_headers

price_headers = variables.price_headers
price_dtype = variables.price_dtype

engine = variables.engine

Date = datetime.datetime.today().strftime('%Y-%m-%d')


Testmode = False
Limit = True

class AllBarOneSiteSpider(scrapy.Spider):
    name='allbarone-sites'
    site_out=[]

    allowed_domains=allowed_domains
    start_urls=[url]

    def parse(self, response):
        yield Request(response.url, 
                        method='GET', 
                        callback=self.parse_detail)

    def parse_detail(self, response):
        json_response=json.loads(response.text)
        venues=json_response

        for venue in venues:
            self.site_out.append(venue)
            yield venue

class MBsSites(scrapy.Spider):
    name = 'mbs-sites-formatted'
    start_urls = [
        "https://www.vintageinn.co.uk/cborms/pub/brands/8/outlets",
        "https://www.sizzlingpubs.co.uk/cborms/pub/brands/109/outlets",
        "https://www.allbarone.co.uk/cborms/pub/brands/17/outlets",
        "https://www.harvester.co.uk/cborms/pub/brands/9/outlets",
        "https://www.emberinns.co.uk/cborms/pub/brands/101/outlets",
        "https://www.tobycarvery.co.uk/cborms/pub/brands/524/outlets",
        "https://www.nicholsonspubs.co.uk/cborms/pub/brands/112/outlets",
        "https://www.oneills.co.uk/cborms/pub/brands/521/outlets",
        "https://www.millerandcarter.co.uk/cborms/pub/brands/37/outlets",
        "https://www.browns-restaurants.co.uk/cborms/pub/brands/31/outlets",
        "https://www.stonehouserestaurants.co.uk/cborms/pub/brands/14/outlets"
    ]

    site_out = []

    def parse(self, response):
        for u in self.start_urls:
            yield Request(u,
                          method='GET',
                          callback=self.parse_venues,
                          meta={'URL': u},
                          dont_filter=True)

    def parse_venues(self, response):
        json_response = json.loads(response.text)
        for Site in json_response:
            Site['Company'] = 'Mitchells & Butlers'
            Site['Format'] = Site['brand']['settings']['domain'].split(".")[0]
            Site['Latitude'] = Site['gpsCoordinates']['latitude']
            Site['Longitude'] = Site['gpsCoordinates']['longitude']
            Site['Address Line 1'] = Site['address']['line1']
            Site['Town'] = Site['address']['town']
            try:
                Site['County'] = Site['address']['county']
            except:
                Site['County'] = 'Not Given'
            Site['Country'] = Site['address']['country']
            Site['Postcode'] = Site['address']['postcode']
            Site['Date'] = Date
            Site['Site Number'] = f'''MB-{Site['bunCode']}'''
            Site = helpers.keep_items(Site, keep_s_list)
            Site = helpers.rename_items(Site, alter_s_list)

            self.site_out.append(Site)
            yield Site

    def closed(self, reason):
        Site_Data = pd.json_normalize(self.site_out)
        Site_Data = Site_Data[site_headers]
        Site_Data = Site_Data.astype(str)
        print(Site_Data.head(5).to_string())
        Site_Data = Site_Data.replace(r'^\s*$', np.nan, regex=True)
        if Site_Data is not None:
            database_Output = 'competitor_sites'
            Site_Data.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000, dtype=site_dtype)
        else:
            print("Empty Dataframe")
            pass

class AB1MenuPricing(scrapy.Spider):
    name = 'mbs-prices-formatted'
    start_urls = [
        "https://www.vintageinn.co.uk/cborms/pub/brands/8/outlets",
        "https://www.sizzlingpubs.co.uk/cborms/pub/brands/109/outlets",
        "https://www.allbarone.co.uk/cborms/pub/brands/17/outlets",
        "https://www.harvester.co.uk/cborms/pub/brands/9/outlets",
        "https://www.emberinns.co.uk/cborms/pub/brands/101/outlets",
        "https://www.tobycarvery.co.uk/cborms/pub/brands/524/outlets",
        "https://www.nicholsonspubs.co.uk/cborms/pub/brands/112/outlets",
        "https://www.oneills.co.uk/cborms/pub/brands/521/outlets",
        "https://www.millerandcarter.co.uk/cborms/pub/brands/37/outlets",
        "https://www.browns-restaurants.co.uk/cborms/pub/brands/31/outlets",
        "https://www.stonehouserestaurants.co.uk/cborms/pub/brands/14/outlets"
    ]

    out = pd.DataFrame()

    def parse(self, response):
        for u in self.start_urls:
            time.sleep(random.randint(0, 1))
            yield scrapy.Request(u,
                          method='GET',
                          callback=self.parse_venues,
                          meta={'URL': u})

    def parse_venues(self, response):
        venues = json.loads(response.text)
        URL = response.meta['URL']
        if Testmode is True:
            venues = venues[0:1]
        for venue in venues:
            bunCode = venue['bunCode']
            pubName = venue['name']
            Site_URl = f'''{response.meta['URL']}/{bunCode}'''

            time.sleep(random.randint(0, 1))
            yield Request(Site_URl,
                          method='GET',
                          callback=self.parse_security,
                          meta={'bunCode':bunCode,'pubName':pubName, 'URL': response.meta['URL']})

    def parse_security(self, response):
        json_response = json.loads(response.text)
        pages = json_response['outletStructure']['pageStructures']

        for page in pages:
            if page['name'].lower() == "oat2":
                uri = page['uri']
                urlI = f'''https://www.{response.meta['URL'].split(".co.uk", 1)[0].split("www.", 1)[1]}.co.uk{uri}'''

                time.sleep(random.randint(0, 2))
                yield Request(urlI,
                              method='GET',
                              callback=self.menu_detail,
                              meta={'bunCode': response.meta['bunCode'],'pubName': response.meta['pubName'], 'URL': response.meta['URL']})

    def menu_detail(self, response):
        json_response = helpers.obj_to_json('Estate', response)

        bunCode = response.meta['bunCode']
        pubName = response.meta['pubName']
        mId = json_response['MarketingBrand']['ScalableId']
        oBunCode = json_response['Outlet']['BunCode']
        oId = json_response['Outlet']['ScalableId']
        oSalesAreaId = json_response['Outlet']['ScalableSalesAreaId']
        URL_Edit = response.meta['URL'].split("outlets", 1)[0]
        urlM = "{0}scalable/{1}/outlets/{2}/{3}/{4}/menus/OAT/menu".format(URL_Edit, mId, oBunCode, oId, oSalesAreaId)

        time.sleep(random.randint(0, 2))
        yield Request(urlM,
                      method='GET',
                      callback=self.menus,
                      meta={'bunCode': bunCode, 'pubName': pubName, 'mId': mId, 'oBunCode': oBunCode, 'oId': oId,
                            'oSalesAreaId': oSalesAreaId, 'URL': URL_Edit})

    def menus(self, response):
        menus = json.loads(response.text)

        for menu in menus:
            desc = menu['menuName']
            y_keys = ['drink', 'draught']
            n_keys = ['hot', 'soft', 'kids', '2 pint serves', '2 Pint Serves and Pitcher']

            n_keys = ['hot', 'soft', 'kids', '2 pint serves']
            y=re.compile("|".join(y_keys))
            n=re.compile("|".join(n_keys))

            if not n.search(desc.lower()):
                bunCode = response.meta['bunCode']
                pubName = response.meta['pubName']
                mId = response.meta['mId']
                oBunCode = response.meta['oBunCode']
                oId = response.meta['oId']
                oSalesAreaId = response.meta['oSalesAreaId']
                menuId = menu['menuId']

                Cat_URL = f'''{response.meta['URL']}scalable/{mId}/outlets/{oBunCode}/{oId}/{oSalesAreaId}/menus/{menuId}/categories'''

                time.sleep(random.randint(0, 2))
                yield Request(Cat_URL,
                              method='GET',
                              callback=self.category_detail,
                              meta={'bunCode': bunCode, 'pubName': pubName, 'mId': mId, 'oBunCode': oBunCode, 'oId': oId,
                                'oSalesAreaId': oSalesAreaId, 'URL': Cat_URL})

    def category_detail(self, response):
        categories = json.loads(response.text)

        bunCode = response.meta['bunCode']
        pubName = response.meta['pubName']
        mId = response.meta['mId']
        oBunCode = response.meta['oBunCode']
        oId = response.meta['oId']
        oSalesAreaId = response.meta['oSalesAreaId']

        Menu_Name = categories['name']
        menuId = categories['menuId']


        for sub_menus in categories['menuSection']:
            desc = sub_menus['name']
            sub_ID = sub_menus['sectionId']
            if desc.lower() != '2 pint serves':
                sub_URL = f'''{response.meta['URL']}/{sub_ID}/items'''

                time.sleep(random.randint(0, 2))
                yield Request(sub_URL,
                              method='GET',
                              callback=self.prices,
                              meta={'bunCode': bunCode, 'pubName': pubName, 'mId': mId, 'oBunCode': oBunCode, 'oId': oId,
                                'oSalesAreaId': oSalesAreaId, 'Menu Name': Menu_Name, 'URL': response.meta['URL']})

    def prices(self, response):
        prices = json.loads(response.text)

        Company = response.meta['URL'].split(".co.uk", 1)[0].split("www.", 1)[1]

        Data = []
        for price in prices:
            Prod_Name = price['name']
            Menu_ID = price['menuId']
            for products in price['portions']:
                prod_id = Prod_Name + str(products['portionName'])
                products['Company'] = 'Mitchells & Butlers'
                products['Date'] = Date
                products['Format'] = response.meta['URL'].split(".co.uk", 1)[0].split("www.", 1)[1]
                products['Menu ID'] = Menu_ID
                products['Menu Name'] = response.meta['Menu Name']
                products['Product Name'] = Prod_Name
                products['Product ID'] = hashlib.md5(prod_id.encode()).hexdigest()


                products['Site Name'] = response.meta['pubName']
                products['Site Number'] = f'''MB-{response.meta['bunCode']}'''


                products = helpers.delete_items(products, del_p_list)
                products = helpers.rename_items(products, alter_p_list)
                products = helpers.keep_items(products, variables.price_headers_C)

                Data.append(products)
                yield products

        try:
            self.out = self.out.append(pd.json_normalize(Data))
        except:
            pass

        if len(self.out.index) >= 100000:
            out = self.out[variables.price_headers_C]
            database_Output = 'competitor_prices'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False,
                       chunksize=16000,
                       dtype=variables.price_dtype_C)
            self.out = pd.DataFrame()

    def closed(self, reason):
        if self.out is not None:
            print(self.out.head(10).to_string())
            out = self.out[variables.price_headers_C]
            database_Output = 'competitor_prices'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000,
                              dtype=variables.price_dtype_C)

class MBsOtherSites(scrapy.Spider):
    name = 'mbs-other-sites-formatted'

    site_out = []

    def start_requests(self):

        url = 'https://www.ordertoyourtable.co.uk/cborms/pub/brands/160_115_7_30_29/outlets'

        yield scrapy.Request(url,
                             method='GET',
                             callback=self.parse)

    def parse(self, response):
        json_response = json.loads(response.text)

        brands = {
            "160": "Oak Tree Pubs",
            "115": "Castle",
            "7": "Premium Country Pubs",
            "30": "Neighbourhood Pubs"
        }

        for Sites in json_response:
            Brand = 'Mitchells & Butlers'
            for i in brands:
                if str(i) == str(Sites['brand']['id']):
                    print(brands[f'{i}'])
                    Brand = brands[f'{i}']

            Site = {}
            Site['Address Line 1'] = Sites['address']['line1']
            Site['Company'] = 'Mitchells & Butlers'
            Site['Country'] = Sites['address']['country']
            try:
                Site['County'] = Sites['address']['county']
            except:
                Site['County'] = 'Not Given'
            Site['Date'] = Date
            Site['Format'] = Brand
            Site['Given ID'] = Sites['bunCode']
            Site['Latitude'] = Sites['gpsCoordinates']['latitude']
            Site['Longitude'] = Sites['gpsCoordinates']['longitude']
            Site['Other Town'] = Sites['address']['town']
            Site['Postcode'] = Sites['address']['postcode']
            Site['Site Name'] = Sites['name']
            Site['Site Number'] = f'''MB-{Sites['bunCode']}'''
            Site['Status'] = Sites['status']
            Site['Telephone Number'] = Sites['telephoneNumber']
            Site['Town'] = Sites['address']['town']

            self.site_out.append(Site)
            yield Site

    def closed(self, reason):
        Site_Data = pd.json_normalize(self.site_out)
        Site_Data = Site_Data[site_headers]
        Site_Data = Site_Data.astype(str)
        print(Site_Data.head(5).to_string())
        Site_Data = Site_Data.replace(r'^\s*$', np.nan, regex=True)
        if Site_Data is not None:
            database_Output = 'competitor_sites'
            Site_Data.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000,
                             dtype=site_dtype)
        else:
            print("Empty Dataframe")
            pass


class MBsOtherSitesPrices(scrapy.Spider):
    name = 'mbs-other-prices-formatted'

    out = pd.DataFrame()

    def start_requests(self):

        url = 'https://www.ordertoyourtable.co.uk/cborms/pub/brands/160_115_7_30_29/outlets'

        yield scrapy.Request(url,
                             method='GET',
                             callback=self.parse)

    def parse(self, response):
        json_response = json.loads(response.text)

        brands = {
            "160": "Oak Tree Pubs",
            "115": "Castle",
            "7": "Premium Country Pubs",
            "30": "Neighbourhood Pubs"
        }
        if Testmode:
            json_response = json_response[:1]

        for Site in json_response:
            if Site['effectiveOrderAtTableEnabled'] is True:
                url = f'''https://www.{Site['domain']}/cborms/pub/brands/{Site['brand']['id']}/outlets/{Site['bunCode']}'''
                yield scrapy.Request(url,
                                     method='GET',
                                     callback=self.parse_security,
                                     meta={'bunCode': Site['bunCode'], 'pubName': Site['name'], 'URL': Site['domain'], 'Format ID': Site['brand']['id']})

    def parse_security(self, response):
        json_response = json.loads(response.text)
        pages = json_response['outletStructure']['pageStructures']

        for page in pages:
            if page['name'].lower() == "oat2":
                uri = page['uri']
                urlI = f'''https://www.{response.meta['URL']}{uri}'''

                time.sleep(random.randint(0, 2))
                yield Request(urlI,
                              method='GET',
                              callback=self.menu_detail,
                              meta={'bunCode': response.meta['bunCode'], 'pubName': response.meta['pubName'],
                                    'URL': response.meta['URL'], 'Format ID': response.meta['Format ID']})



    def menu_detail(self, response):
        json_response = helpers.obj_to_json('Estate', response)

        bunCode = response.meta['bunCode']
        pubName = response.meta['pubName']
        mId = json_response['MarketingBrand']['ScalableId']
        oBunCode = json_response['Outlet']['BunCode']
        oId = json_response['Outlet']['ScalableId']
        oSalesAreaId = json_response['Outlet']['ScalableSalesAreaId']
        URL_Edit = response.meta['URL'].split("outlets", 1)[0]
        urlM = "https://www.{0}/cborms/pub/brands/{1}/scalable/{2}/outlets/{3}/{4}/{5}/menus/OAT/menu".format(URL_Edit, response.meta['Format ID'], mId, oBunCode, oId, oSalesAreaId)
        time.sleep(random.randint(0, 2))
        yield Request(urlM,
                      method='GET',
                      callback=self.menus,
                      meta={'bunCode': bunCode, 'pubName': pubName, 'mId': mId, 'oBunCode': oBunCode, 'oId': oId,
                            'oSalesAreaId': oSalesAreaId, 'URL': URL_Edit, 'Format ID': response.meta['Format ID']})


    def menus(self, response):
        menus = json.loads(response.text)

        for menu in menus:
            desc = menu['menuName']
            y_keys = ['drink', 'draught']
            n_keys = ['hot', 'soft', 'kids', '2 pint serves', '2 Pint Serves and Pitcher']

            n_keys = ['hot', 'soft', 'kids', '2 pint serves']
            y = re.compile("|".join(y_keys))
            n = re.compile("|".join(n_keys))

            if not n.search(desc.lower()):
                bunCode = response.meta['bunCode']
                pubName = response.meta['pubName']
                mId = response.meta['mId']
                oBunCode = response.meta['oBunCode']
                oId = response.meta['oId']
                oSalesAreaId = response.meta['oSalesAreaId']
                menuId = menu['menuId']

                Cat_URL = f'''https://www.{response.meta['URL']}/cborms/pub/brands/{response.meta['Format ID']}/scalable/{mId}/outlets/{oBunCode}/{oId}/{oSalesAreaId}/menus/{menuId}/categories'''

                time.sleep(random.randint(0, 2))
                yield Request(Cat_URL,
                              method='GET',
                              callback=self.category_detail,
                              meta={'bunCode': bunCode, 'pubName': pubName, 'mId': mId, 'oBunCode': oBunCode, 'oId': oId,
                                    'oSalesAreaId': oSalesAreaId, 'URL': Cat_URL, 'Format ID': response.meta['Format ID']})


    def category_detail(self, response):
        categories = json.loads(response.text)

        bunCode = response.meta['bunCode']
        pubName = response.meta['pubName']
        mId = response.meta['mId']
        oBunCode = response.meta['oBunCode']
        oId = response.meta['oId']
        oSalesAreaId = response.meta['oSalesAreaId']

        Menu_Name = categories['name']
        menuId = categories['menuId']

        for sub_menus in categories['menuSection']:
            desc = sub_menus['name']
            sub_ID = sub_menus['sectionId']
            if desc.lower() != '2 pint serves':
                sub_URL = f'''{response.meta['URL']}/{sub_ID}/items'''

                time.sleep(random.randint(0, 2))
                yield Request(sub_URL,
                              method='GET',
                              callback=self.prices,
                              meta={'bunCode': bunCode, 'pubName': pubName, 'mId': mId, 'oBunCode': oBunCode, 'oId': oId,
                                    'oSalesAreaId': oSalesAreaId, 'Menu Name': Menu_Name, 'URL': response.meta['URL'], 'Format ID': response.meta['Format ID']})


    def prices(self, response):
        prices = json.loads(response.text)

        Company = response.meta['URL'].split(".co.uk", 1)[0].split("www.", 1)[1]

        brands = {
            "160": "Oak Tree Pubs",
            "115": "Castle",
            "7": "Premium Country Pubs",
            "30": "Neighbourhood Pubs"
        }

        print(response.meta['Format ID'])
        Brand = 'Mitchells & Butlers'
        for i in brands:
            if str(i) == str(response.meta['Format ID']):
                print(brands[f'{i}'])
                Brand = brands[f'{i}']

        Data = []
        for price in prices:
            Prod_Name = price['name']
            Menu_ID = price['menuId']
            for products in price['portions']:
                prod_id = Prod_Name + str(products['portionName'])
                products['Company'] = 'Mitchells & Butlers'
                products['Date'] = Date
                products['Format'] = Brand
                products['Menu ID'] = Menu_ID
                products['Menu Name'] = response.meta['Menu Name']
                products['Product Name'] = Prod_Name
                products['Product ID'] = hashlib.md5(prod_id.encode()).hexdigest()

                products['Site Name'] = response.meta['pubName']
                products['Site Number'] = f'''MB-{response.meta['bunCode']}'''

                products = helpers.delete_items(products, del_p_list)
                products = helpers.rename_items(products, alter_p_list)
                products = helpers.keep_items(products, price_headers)

                Data.append(products)
                yield products

        try:
            self.out = self.out.append(pd.json_normalize(Data))
        except:
            pass

        if len(self.out.index) >= 100000:
            out = self.out[price_headers]
            database_Output = 'comeptitor_prices'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000,
                       dtype=price_dtype)
            self.out = pd.DataFrame()


    def closed(self, reason):
        if self.out is not None:
            print(self.out.head(100).to_string())
            out = self.out[price_headers]
            database_Output = 'competitor_prices'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000,
                       dtype=price_dtype)






# class AB1MenuPricingKeyProducts(scrapy.Spider):
#     name = 'mbs-prices-formatted-key-products'
#     start_urls = ["https://www.vintageinn.co.uk/cborms/pub/brands/8/outlets",
#                   "https://www.sizzlingpubs.co.uk/cborms/pub/brands/109/outlets",
#                   "https://www.allbarone.co.uk/cborms/pub/brands/17/outlets",
#                   "https://www.harvester.co.uk/cborms/pub/brands/9/outlets",
#                   "https://www.emberinns.co.uk/cborms/pub/brands/101/outlets",
#                   "https://www.tobycarvery.co.uk/cborms/pub/brands/524/outlets",
#                   "https://www.nicholsonspubs.co.uk/cborms/pub/brands/112/outlets",
#                   "https://www.oneills.co.uk/cborms/pub/brands/521/outlets"]
#
#     data_out = []
#
#     def parse(self, response):
#         for u in self.start_urls:
#             time.sleep(random.randint(1, 3))
#             yield scrapy.Request(u,
#                                  method='GET',
#                                  callback=self.parse_venues,
#                                  meta={'URL': u})
#
#     def parse_venues(self, response):
#         venues = json.loads(response.text)
#         URL = response.meta['URL']
#         if Testmode is True:
#             venues = venues[0:1]
#         for venue in venues:
#             bunCode = venue['bunCode']
#             pubName = venue['name']
#             Site_URl = f'''{response.meta['URL']}/{bunCode}'''
#
#             time.sleep(random.randint(1, 2))
#             yield Request(Site_URl,
#                           method='GET',
#                           callback=self.parse_security,
#                           meta={'bunCode': bunCode, 'pubName': pubName, 'URL': response.meta['URL']})
#
#     def parse_security(self, response):
#         json_response = json.loads(response.text)
#         pages = json_response['outletStructure']['pageStructures']
#
#         for page in pages:
#             if page['name'].lower() == "oat2":
#                 uri = page['uri']
#                 urlI = f'''https://www.{response.meta['URL'].split(".co.uk", 1)[0].split("www.", 1)[1]}.co.uk{uri}'''
#
#                 time.sleep(random.randint(1, 2))
#                 yield Request(urlI,
#                               method='GET',
#                               callback=self.menu_detail,
#                               meta={'bunCode': response.meta['bunCode'], 'pubName': response.meta['pubName'],
#                                     'URL': response.meta['URL']})
#
#     def menu_detail(self, response):
#         json_response = helpers.obj_to_json('Estate', response)
#
#         bunCode = response.meta['bunCode']
#         pubName = response.meta['pubName']
#         mId = json_response['MarketingBrand']['ScalableId']
#         oBunCode = json_response['Outlet']['BunCode']
#         oId = json_response['Outlet']['ScalableId']
#         oSalesAreaId = json_response['Outlet']['ScalableSalesAreaId']
#         URL_Edit = response.meta['URL'].split("outlets", 1)[0]
#         urlM = "{0}scalable/{1}/outlets/{2}/{3}/{4}/menus/OAT/menu".format(URL_Edit, mId, oBunCode, oId, oSalesAreaId)
#
#         time.sleep(random.randint(1, 2))
#         yield Request(urlM,
#                       method='GET',
#                       callback=self.menus,
#                       meta={'bunCode': bunCode, 'pubName': pubName, 'mId': mId, 'oBunCode': oBunCode, 'oId': oId,
#                             'oSalesAreaId': oSalesAreaId, 'URL': URL_Edit})
#
#     def menus(self, response):
#         menus = json.loads(response.text)
#
#         for menu in menus:
#             desc = menu['menuName']
#
#             y_keys = ['drink', 'draught']
#             n_keys = ['hot', 'soft', 'kids', '2 pint serves']
#
#             y = re.compile("|".join(y_keys))
#             n = re.compile("|".join(n_keys))
#
#             if not n.search(desc.lower()):
#                 bunCode = response.meta['bunCode']
#                 pubName = response.meta['pubName']
#                 mId = response.meta['mId']
#                 oBunCode = response.meta['oBunCode']
#                 oId = response.meta['oId']
#                 oSalesAreaId = response.meta['oSalesAreaId']
#                 menuId = menu['menuId']
#
#                 Cat_URL = f'''{response.meta['URL']}scalable/{mId}/outlets/{oBunCode}/{oId}/{oSalesAreaId}/menus/{menuId}/categories'''
#
#                 time.sleep(random.randint(0, 2))
#                 yield Request(Cat_URL,
#                               method='GET',
#                               callback=self.category_detail,
#                               meta={'bunCode': bunCode, 'pubName': pubName, 'mId': mId, 'oBunCode': oBunCode,
#                                     'oId': oId,
#                                     'oSalesAreaId': oSalesAreaId, 'URL': Cat_URL})
#
#     def category_detail(self, response):
#         categories = json.loads(response.text)
#
#         bunCode = response.meta['bunCode']
#         pubName = response.meta['pubName']
#         mId = response.meta['mId']
#         oBunCode = response.meta['oBunCode']
#         oId = response.meta['oId']
#         oSalesAreaId = response.meta['oSalesAreaId']
#
#         Menu_Name = categories['name']
#         menuId = categories['menuId']
#
#         for sub_menus in categories['menuSection']:
#             desc = sub_menus['name']
#             sub_ID = sub_menus['sectionId']
#             if desc.lower() != '2 pint serves':
#                 sub_URL = f'''{response.meta['URL']}/{sub_ID}/items'''
#
#                 time.sleep(random.randint(1, 2))
#                 yield Request(sub_URL,
#                               method='GET',
#                               callback=self.prices,
#                               meta={'bunCode': bunCode, 'pubName': pubName, 'mId': mId, 'oBunCode': oBunCode,
#                                     'oId': oId,
#                                     'oSalesAreaId': oSalesAreaId, 'Menu Name': Menu_Name, 'URL': response.meta['URL']})
#
#     def prices(self, response):
#         prices = json.loads(response.text)
#
#         Company = response.meta['URL'].split(".co.uk", 1)[0].split("www.", 1)[1]
#
#         y = list(helpers.Key_Product_Matching(Company, Key_Products))
#         y = y[0]
#
#         Key_Products_Filtered = Key_Products[['Stonegate Product', f'{Company} ID']]
#         Key_Products_Filtered.dropna(inplace=True)
#
#         for price in prices:
#             Prod_Name = price['name']
#             Menu_ID = price['menuId']
#             for products in price['portions']:
#                 prod_id = Prod_Name + str(products['portionName'])
#                 products['Date'] = Date
#                 products['Product Name'] = Prod_Name
#                 products['Product ID'] = hashlib.md5(prod_id.encode()).hexdigest()
#                 products['Menu Name'] = response.meta['Menu Name']
#                 products['Menu ID'] = Menu_ID
#                 products['Site Name'] = response.meta['pubName']
#                 products['Site Number'] = f'''MB-{response.meta['bunCode']}'''
#                 products['Company'] = 'Mitchells & Butlers'
#
#                 products['Format'] = response.meta['URL'].split(".co.uk", 1)[0].split("www.", 1)[1]
#
#                 products = helpers.delete_items(products, del_p_list)
#                 products = helpers.rename_items(products, alter_p_list)
#                 products = helpers.keep_items(products, price_headers)
#
#                 if products['Product ID'] in y:
#                     products['Stonegate Product'] = helpers.Key_Products_To_Json(Key_Products_Filtered, products['Product ID'])
#                     self.data_out.append(products)
#                     yield products
#
#     def closed(self, reason):
#         if self.data_out:
#             Price_Data = pd.json_normalize(self.data_out)
#             Price_Data = Price_Data[price_headers]
#             print(Price_Data.head(10).to_string())
#             if Price_Data is not None:
#                 database_Output = 'Competitor_Pricing'
#                 Price_Data.to_sql(name=database_Output, con=engine, if_exists='append', index=False, chunksize=16000,
#                                   dtype=price_dtype)
#             else:
#                 print("Empty Dataframe")
#                 pass

class MBsOpeningTimes(scrapy.Spider):
    name = 'MBS-Opening-Times'
    start_urls = ["https://www.vintageinn.co.uk/cborms/pub/brands/8/outlets",
                "https://www.sizzlingpubs.co.uk/cborms/pub/brands/109/outlets",
                "https://www.allbarone.co.uk/cborms/pub/brands/17/outlets",
                "https://www.harvester.co.uk/cborms/pub/brands/9/outlets",
                "https://www.emberinns.co.uk/cborms/pub/brands/101/outlets",
                "https://www.tobycarvery.co.uk/cborms/pub/brands/524/outlets",
                "https://www.nicholsonspubs.co.uk/cborms/pub/brands/112/outlets",
                "https://www.oneills.co.uk/cborms/pub/brands/521/outlets",
                "https://www.millerandcarter.co.uk/cborms/pub/brands/37/outlets",
                "https://www.browns-restaurants.co.uk/cborms/pub/brands/31/outlets",
                "https://www.stonehouserestaurants.co.uk/cborms/pub/brands/14/outlets"]

    out = pd.DataFrame()

    def start_requests(self):
        urls = self.start_urls
        for u in urls:
            yield Request(u,
                          method='GET',
                          callback=self.parse_venues,
                          meta={'URL': u},
                          dont_filter=True)
            time.sleep(random.randint(1, 3))

    def parse_venues(self, response):
        json_response = json.loads(response.text)

        for Site in json_response:
            url = response.meta['URL']
            url = url + '/' + str(Site['bunCode'])
            time.sleep(random.randint(0, 1))
            yield scrapy.Request(url,
                                 callback=self.parse)
            time.sleep(random.randint(1, 2))

    def parse(self, response):
        json_response = json.loads(response.text)
        # print(json_response['bunCode'])
        for day in json_response['effectiveOpeningTimes']['periods']:
            OT = {}
            OT['Site Number'] = f'''MB-{json_response['bunCode']}'''
            OT['Date'] = Date
            OT['Day'] = day['days']['text']
            O = day['times'][0]['timeFrom'].split(":")
            C = day['times'][0]['timeTo'].split(":")
            OT['Open'] = f'{O[0]}:{O[1]}'
            OT['Close'] = f'{C[0]}:{C[1]}'

            yield OT
            self.out = self.out.append(pd.json_normalize(OT))


    def closed(self, reason):
        if self.out is not None:
            print(self.out.head(100).to_string())
            out = self.out[['Site Number', 'Date', 'Day', 'Open', 'Close']]
            database_Output = 'COMPETITOR_OPENING_TIMES'
            out.to_sql(name=database_Output, con=variables.engine_C, if_exists='append', index=False, chunksize=16000)