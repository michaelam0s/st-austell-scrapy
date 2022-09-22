#scrapy crawl revs-sites -L WARN -o test.json
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd

#product details
products=['carlsberg','bud light','carling','fosters','tennents',
          'coors light','amstel','stella','estrella','san miguel',
          'heineken','peroni','birra moretti','brewdog punk',
          'strongbow','john smiths','doom bar','guinness', 'John Smiths', 'darkfruit', 'Budweiser Btl', 'Rekord Sp Plum', 'J Smith Ex Smt', 'Diet Pepsi', 'Punk IPA 5.2', 'Diet Pepsi',
          'Bt Kopp Passionf', 'Gordons Gin', 'Sambuca Classic', 'J2O App & Mango', 'Hardy Bin Chard', 'Large Prosecco']

d_products=['john smiths','strongbow','heineken','kronenbourg','san miguel','stella artois',
          'coors light','bud light','carling','carlsberg','fosters','guinness','corona',
          'kopparberg','smirnoff ice','diet pepsi','smirnoff','chocolate brownie',
          'all day breakfast','cheese & bacon burger','fish & chips','ham & chips',
          'lasagne','scampi & chips','bbq chicken melt','mixed grill','nachos',
          'and chips', 'chips', 'ham']

GK_products = ['Bud Light', 'Carling', 'Carlsberg', 'Fosters', 'Tennents', 'Amstel Draught', 'Coors Light',
              'Heineken', 'San Miguel Drght', 'Stella Artois Dr', 'Birra Moretti', 'Estrella D Keg',
              'Peroni Draught', 'Punk IPA', 'Camden Hells', 'John Smiths Smth', 'Doom Bar', 'Guinness', 'Strongbow',
              'Stgbow DARKFRUIT', 'Aspalls 5.5% Suf', 'Budweiser Btl', 'Corona 330ml', 'Peroni Libera', 'Rekord Sp Plum',
              'WKD Blue', 'Gordons Gin', 'Smirnoff Red', 'Diet Coke 6oz', 'Slim Tonic 200', '25ml Antica Clas',
              'Jager Bomb', 'Glass Pornstar', 'Diet Coke REG', 'J20 Apple Rasp', 'Red Bull', 'D Giotto Merlot',
              'D Amodo Prosecco', 'Chkn Wing 10', 'BGR Beef', 'BGR Cheese', 'BGR Chse & Bacon', 'Cod & Chips',
              'Scampi & Chips', 'Beef & Ale Pie', 'Hunters Chicken', 'Roast Beef MM', 'Pizza Pepperoni', 'Chkn Club',
               'Choc Fudge Cake', 'Tea', 'burger']



# d_products=['.*']
products_combined = products + d_products + GK_products

# site export formatting

site_headers = ['Address Line 1', 'Company', 'Format', 'Country', 'Date', 'Given ID', 'Latitude', 'Longitude', 'Other Town', 'Postcode', 'Site Name', 'Site Number',
             'Status', 'Telephone Number', 'Town']

site_dtype = {'Address Line 1': sqlalchemy.VARCHAR(length=1000),
           'Company': sqlalchemy.VARCHAR(length=1000),
           'Format': sqlalchemy.VARCHAR(length=1000),
           'Country': sqlalchemy.VARCHAR(length=1000),
           'Date': sqlalchemy.VARCHAR(length=1000),
           'Given ID': sqlalchemy.VARCHAR(length=1000),
           'Latitude': sqlalchemy.VARCHAR(length=1000),
           'Longitude': sqlalchemy.VARCHAR(length=1000),
           'Other Town': sqlalchemy.VARCHAR(length=1000),
           'Postcode': sqlalchemy.VARCHAR(length=1000),
           'Site Name': sqlalchemy.VARCHAR(length=1000),
           'Site Number': sqlalchemy.VARCHAR(length=1000),
           'Status': sqlalchemy.VARCHAR(length=1000),
           'Telephone Number': sqlalchemy.VARCHAR(length=1000),
           'Town': sqlalchemy.VARCHAR(length=1000)}

# pricing export formatting
price_headers = ['Date', 'Product Name', 'Price', 'Portion Size', 'Product ID', 'Menu Name', 'Menu ID', 'Site Name',
             'Site Number', 'Company', 'Format']

price_headers_C = ['Date', 'Product Name', 'Price', 'Portion Size', 'Product ID', 'Menu Name', 'Menu ID', 'Site Name',
             'Site Number', 'Company', 'Format', 'Calories']

price_dtype = {'Date': sqlalchemy.VARCHAR(length=1000),
           'Product Name': sqlalchemy.VARCHAR(length=1000),
           'Price': sqlalchemy.VARCHAR(length=1000),
           'Portion Size': sqlalchemy.VARCHAR(length=1000),
           'Product ID': sqlalchemy.VARCHAR(length=1000),
           'Menu Name': sqlalchemy.VARCHAR(length=1000),
           'Menu ID': sqlalchemy.VARCHAR(length=1000),
           'Site Name': sqlalchemy.VARCHAR(length=1000),
           'Site Number': sqlalchemy.VARCHAR(length=1000),
           'Company': sqlalchemy.VARCHAR(length=1000),
           'Format': sqlalchemy.VARCHAR(length=1000)}

price_dtype_C = {'Date': sqlalchemy.VARCHAR(length=1000),
           'Product Name': sqlalchemy.VARCHAR(length=1000),
           'Price': sqlalchemy.VARCHAR(length=1000),
           'Portion Size': sqlalchemy.VARCHAR(length=1000),
           'Product ID': sqlalchemy.VARCHAR(length=1000),
           'Menu Name': sqlalchemy.VARCHAR(length=1000),
           'Menu ID': sqlalchemy.VARCHAR(length=1000),
           'Site Name': sqlalchemy.VARCHAR(length=1000),
           'Site Number': sqlalchemy.VARCHAR(length=1000),
           'Company': sqlalchemy.VARCHAR(length=1000),
           'Format': sqlalchemy.VARCHAR(length=1000),
            'Calories': sqlalchemy.VARCHAR(length=1000)

                 }

# general output
engine = create_engine(
    'snowflake://{user}:{password}@{account}/{database}/{schema}'.format(
        user='ZYTE_INGESTION',
        password='4ewa6P5SUTQ8YUm',
        account='yd02573.west-europe.azure',
        database='PROD_WAREHOUSE',
        schema='TEMP'
    )
)
engine_C = create_engine(
    'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}'.format(
        user='ZYTE_INGESTION',
        password='4ewa6P5SUTQ8YUm',
        account='yd02573.west-europe.azure',
        database='PROD_WAREHOUSE',
        schema='INGESTION_COMPETITOR_DATA',
        warehouse='PROD_INGESTION_STG',
        role='PROD_INGESTION'
    )
)

# scrapinghub keys
shubkey="1ee78fe651784482b824b851caa7d631"
# projectID="541209"
projectID="621283"

proxy="http://b4af3126344147d081378070993528bb:@proxy.crawlera.com:8010/"

p_headers = {"x-forwarded-proto": "https",
              "x-forwarded-port": "443",
              "host": "postman-echo.com",
              "x-amzn-trace-id": "Root=1-5f0c3bf8-65b94fcc4313b2bff5c9741a",
              "user-agent": "PostmanRuntime/7.25.0",
              "accept": "*/*",
              "cache-control": "no-cache",
              "postman-token": "1acdbe88-16c0-44d5-b942-4452a65128ff",
              "accept-encoding": "gzip, deflate, br"
}

b_headers = {'Connection': 'keep-alive',
              'Cache-Control': 'max-age=0',
              'DNT': '1',
              'Upgrade-Insecure-Requests': '1',
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36',
              'Sec-Fetch-User': '?1',
              'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
              'Sec-Fetch-Site': 'same-origin',
              'Sec-Fetch-Mode': 'navigate',
              'Accept-Encoding': 'gzip, deflate, br',
              'Accept-Language': 'en-US,en;q=0.9'
}

#eigroup
e_url='https://roundapp.azurewebsites.net/api/'
e_allowed_domains=['azurewebsites.net']

#wetherspoons
w_url='https://static.wsstack.nn4maws.net/v1/venues/en_gb/venues.json'
w_allowed_domains=['iopapi.zonalconnect.com']
w_headers={'Accept-Encoding': "gzip",
            'Connection': "Keep-Alive",
            'Host': "static.wsstack.nn4maws.nat",
            'User-Agent': "Wetherspoon/3.3.7 Dalvik/2.1.0 (Linux; U; Android 7.1.1; Google Pixel XL Build/NMF26Q)",
            'Cache-Control': "max-age=0"}

w_usr="jdw-nn4m-order-n-pay"
w_pwd="lleg4r-des5spr1t5-bu5ters"

w_payload = {
  "request" : {
    "username" : w_usr,
    "password" : w_pwd
  }
}

w_s_alter_list = {'name': 'Site Name', 'iOrderId': 'Given ID', 'long': 'Longitude', 'lat': 'Latitude', 'country': 'Country', 'town': 'Town', 'postcode': 'Postcode', 'line1': 'Address Line 1', 'county': 'County'}

w_s_keep_list = ['name', 'iOrderId', 'long', 'lat', 'country', 'town', 'postcode', 'line1', 'county', 'Site Number', 'Status']

w_del_list = ['allergenList', 'canAddOn', 'chilliHeat', 'commonTillRequests', 'defaultCourseId', 'defaultPortionId', 'description', 'displayCalories',
              'displayName', 'excludeAddOn', 'filterOptions', 'hasInfo', 'iconsToShow', 'image', 'includesADrink', 'leadingIcons', 'minimumAge', 'portions',
              'displayPrice', 'promoText', 'variantId', 'choices', 'iOrderDisplayId', 'addOnList', 'additionalPortionOptions', 'wineGroup', 'promoDisplayPrice', 'displayPriceLabel', 'promoPriceLabel', 'promoPriceValue',
              'canReorder', 'isSpecial', 'prePriceLabel', 'extendedRange']

w_alter_list = {
    "defaultPortionName": "Portion Size", "priceValue": "Price", "showPortion": "Show Portion", "showCTR": "Shown", "eposName": "Product Name", "iOrder_ID": "Site Number", "menuId": "Menu ID",
    "productId": "Product ID"
}

#Mitchel and Butlers
mb_s_alter_list = {'bunCode': 'Given ID',
          'name': 'Site Name',
          'status': 'Status',
          'telephoneNumber': 'Telephone Number',
          'line1': 'Address Line 1',
          'town': 'Town',
          'country': 'Country',
          'postcode': 'Postcode',
          'latitude': 'Latitude',
          'longitude': 'Longitude',
          'longName': 'Other Town'
          }

mb_s_del_list = ['address', 'gpsCoordinates', 'outletFeatures', 'domain', 'county']

mb_s_keep_list = ['bunCode', 'name', 'longName', 'status', 'telephoneNumber', 'Company', 'Format', 'Latitude', 'Longitude', 'Address Line 1', 'Town', 'Country', 'Postcode', 'Date', 'Site Number', 'Other Town']

mb_del_list = ['portionFriendlyName', 'portionTypeId', 'modifierGroups']

mb_alter_list = {"portionName": "Portion Size", "price": "Price", 'energyKcalPerPortion': 'Calories'}

#all bar one
a_url='https://www.allbarone.co.uk/cborms/pub/brands/17/outlets'
a_allowed_domains=['allbarone.co.uk']
a_menus = ["Brunch", "Hot", "Kids"]

#revs - partial open
r_urlS="https://api.pepperhq.com/"
r_urlM="https://menu.pepperhq.com/"   
r_allowed_domains=['pepperhq.com']
r_headers={'x-application-id': "5r020wdw9s"}
r_tid="5d133e6f31c82b48cd180fab"

#youngs - hasn't launched yet
y_urlS="https://api.pepperhq.com/"
y_urlM="https://menu.pepperhq.com/" 
y_allowed_domains=['pepperhq.com']
y_headers={'x-application-id': "xr2c78t9dk"}

y_s_alter_list = {'_id': 'Site Number', 'title': 'Site Name', 'address': 'Address Line 1'}
y_s_keep_list = []

#heineken - partial launch
h_url="https://api-prod.swifty-app.co/api/1.0/"
h_allowed_domains=['api-prod.swifty-app.co']

hk_s_del_list = []
hk_s_keep_list = []
hk_s_alter_list = {}

#marstons - incomplete
m_urlS="https://api-cdn.orderbee.co.uk/brand"
m_urlM="https://api-cdn.orderbee.co.uk/venues/{0}"
m_allowed_domains=['orderbee.co.uk']
m_headers = {'Origin': 'https://order.marstons.co.uk'}

m_bulk_headers = {
  'Connection': 'keep-alive',
  'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
  'Accept': 'application/json, text/javascript, */*; q=0.01',
  'X-Requested-With': 'XMLHttpRequest',
  'sec-ch-ua-mobile': '?0',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
  'Sec-Fetch-Site': 'same-origin',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Dest': 'empty',
  'Referer': 'https://www.marstonspubs.co.uk/pubs/finder/',
  'Accept-Language': 'en-GB,en;q=0.9,en-US;q=0.8'
}

m_s_alter_list_bulk = {'phc': 'Given ID', 'name': 'Site Name', 'town': 'Town', 'lat': 'Latitude', 'lng': 'Longitude'}



m_s_del_list = ['_id', 'address', 'description', 'hideAddress', 'isEnabled', 'logo', 'shortAddress', 'slug']
m_s_keep_list = []

m_del_list = ['name', 'choiceGroups', 'sku', 'id', '_id', '__v', 'createdAt', 'updatedAt', 'description', '__type', '_type', 'diet']
m_alter_list = {"price": "Price", "isActive": "Show Portion", "shortName": "Product Name", "inStock": "Shown"}

#StarPubs
SP_headers = {
  'Content-Type': 'application/x-www-form-urlencoded',
  'Accept': 'application/json',
  'User-Agent': 'com.heineken.starorder Dalvik/2.1.0 (Linux; U; Android 6.0; Google Nexus 6_2 Build/MRA58K)',
  'Host': 'iopapi.zonalconnect.com',
  'Connection': 'Keep-Alive',
  'Accept-Encoding': 'gzip',
  'Cookie': 'CFID=209742285; CFTOKEN=ff55c3d8c028303e-27974868-5056-AF31-C44019C3660D55A2'
}

SP_Site_Payload = {
  "request": {
    "username": "star-pubs-wla",
    "password": "6x8a6gqj",
    "version": "1.0.0",
    "bundleIdentifier": "com.heineken.starorder",
    "platform": "Android",
    "userDeviceIdentifier": "29884031-88a4-434c-88a4-216a8f1e7cb4",
    "method": "venues"
  }
}

SP_Menu_headers = {
  'Content-Type': 'application/x-www-form-urlencoded',
  'Accept': 'application/json',
  'User-Agent': 'com.heineken.starorder Dalvik/2.1.0 (Linux; U; Android 6.0; Google Nexus 6_2 Build/MRA58K)',
  'Host': 'iopapi.zonalconnect.com',
  'Connection': 'Keep-Alive',
  'Accept-Encoding': 'gzip'
}

#cityclub
c_url = "https://app-api.uk.loke.global/graphql"
c_allowed_domains=['app-api.uk.loke.global']
c_headers={'tidy-app-key':'b54f4667-aeb0-4cf6-a1d5-0da4a8ec84c4', 'Content-Type': 'application/json'}

c_qloc = "query LocationsQuery($currentCoords: CoordsInput) {\n  merchant {\n    locations(myLocation: $currentCoords) {\n      id\n      name\n      coords {\n        latitude: lat\n        longitude: lng\n        __typename\n      }\n      address {\n        street\n        street2\n        locality\n        region\n        postalCode\n        country\n        __typename\n      }\n      website\n      phoneNumber\n      openingHours\n      itemCollectionCode\n      paymentOptions {\n        __typename\n        id\n        label\n        ... on WebOrderingPaymentOption {\n          url\n          __typename\n        }\n        ... on DeliveryPaymentOption {\n          deliveryLocationConfirmText: locationConfirmText\n          introText\n          __typename\n        }\n        ... on OrderingPaymentOption {\n          orderingRefPromptText: refPromptText\n          orderingScanner: scanner\n          orderingScannerTitle: scannerTitle\n          locationConfirmText\n          waitTimeText\n          __typename\n        }\n        ... on BillPaymentOption {\n          billRefPromptText: refPromptText\n          billScanner: scanner\n          billScannerTitle: scannerTitle\n          __typename\n        }\n      }\n      __typename\n    }\n    currency\n    currencyDecimalDigits\n    __typename\n  }\n}\n"
c_qmen = "query OrderingQuery($location: String!) {\n  ordering(location: $location) {\n    waitTime\n    status\n    menu {\n      name\n      products {\n        id\n        name\n        description\n        imageUrl\n        amount\n        tags\n        options {\n          id\n          name\n          minSelect\n          maxSelect\n          choices {\n            id\n            name\n            amount\n            selected\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    tables\n    delivery {\n      feeAmount\n      minimumAmount\n      postcodes\n      minutesMin\n      minutesMax\n      __typename\n    }\n    __typename\n  }\n}\n"

c_payload={
  "operationName" : "",
  "variables": {},
  "query": ""
}

cc_s_alter_list = {'id': 'Given ID', 'name': 'Site Name', 'phoneNumber': 'Telephone Number'}
cc_s_keep_list = ['Address Line 1', 'Latitude', 'Longitude', 'Given ID', 'Site Name', 'Telephone Number', 'Company', 'Format', 'Country', 'County', 'Date', 'Site Number', 'Other Town', 'Postcode', 'Status', 'Town']

#whatpub
wp_urlS="https://whatpub.com/api/1/SearchPubs"
wp_urlD="https://whatpub.com/api/1/GetPubDetails" 
wp_headers={'Content-Type': 'application/x-www-form-urlencoded',}
wp_allowed_domains=['whatpub.com']

WP_TOKEN = "zmg8BsY1TnEg52RPdUZZCDunfeizAI94uOrI5oid"
wp_payload = {
   "_token":WP_TOKEN,
    "Token":WP_TOKEN, 
     "Limit":999999,
}

engine_Example = create_engine(
    'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}'.format(
        user='ISAACRICKETTS',
        password='',
        account='yd02573.west-europe.azure',
        database='PROD_WAREHOUSE',
        schema='CONSUMPTION_EXTERNAL_DATA',
        warehouse='PROD_INGESTION_STG',
        role='PROD_INGESTION'
    )
)

#greene king
g_url='https://iopapi.zonalconnect.com/'
g_allowed_domains=['iopapi.zonalconnect.com']
g_headers={'content-type': "application/x-www-form-urlencoded"}

g_usr="d12452a32886891"
g_pwd="d025675694209d"
g_ver="1.10.0"
g_bid="com.greeneking.orderandpay"
# request=%7B%22request%22%3A%20%7B%22username%22%3A%20%22d12452a32886891%22%2C%20%22password%22%3A%20%22d025675694209d%22%2C%20%22version%22%3A%20%221.10.0%22%2C%20%22bundleIdentifier%22%3A%20%22com.greeneking.orderandpay%22%2C%20%22method%22%3A%20%22venues%22%2C%20%22userDeviceIdentifier%22%3A%20%22lwnyntx-fu6z-rk0k-v1z36h9cxax6%22%7D%7D

g_payload={
  "request" : {
    "username" : g_usr,
    "password" : g_pwd,
    "version": g_ver,
    "bundleIdentifier": g_bid
  }
}

g_s_alter_list = {'line1': 'Address Line 1', 'id': 'Given ID', 'name': 'Site Name'}
g_s_keep_list = ['Address Line 1', 'Format', 'Company', 'Country', 'County', 'Date', 'Given ID', 'Latitude', 'Longitude', 'Other Town', 'Postcode', 'Site Name', 'Site Number', 'Status', 'Telephone Number', 'Town']

g_del_list = ['id', 'choices', 'portion_name', 'salesAreaId', 'supplementPrice']
g_alter_list = {"price": "Price", "name": "Portion Size"}

# st Austell
sta_url='https://iopapi.zonalconnect.com/'
sta_allowed_domains=['iopapi.zonalconnect.com']
sta_headers={'content-type': "application/x-www-form-urlencoded"}
# "request=%7B%22request%22%3A%7B%22username%22%3A%22staustell_wla%22%2C%22password%22%3A%22xzvvtIHR%22%2C%22version%22%3A%221.4.0%22%2C%22bundleIdentifier%22%3A%22uk.co.staustellbrewery.iorder%22%2C%22platform%22%3A%22Android%22%2C%22userDeviceIdentifier%22%3A%22a75be58f-7f0f-4407-9be6-8cbad086d5eb%22%2C%22method%22%3A%22venues%22%7D%7D"
# uk.co.staustellbrewery.iorder Dalvik/2.1.0 (Linux; U; Android 6.0; SCRAPERS SNOOPING Build/MRA58K)

sta_usr="staustell_wla"
sta_pwd="xzvvtIHR"
sta_ver="1.4.0"
sta_bid="uk.co.staustellbrewery.iorder"

# body="request=%7B%22request%22%3A%7B%22username%22%3A%22staustell_wla%22%2C%22password%22%3A%22xzvvtIHR%22%2C%22version%22%3A%221.4.0%22%2C%22bundleIdentifier%22%3A%22uk.co.staustellbrewery.iorder%22%2C%22platform%22%3A%22Android%22%2C%22userDeviceIdentifier%22%3A%22a75be58f-7f0f-4407-9be6-8cbad086d5eb%22%2C%22method%22%3A%22venues%22%7D%7D", 

sta_payload={
  "request" : {
    "username" : sta_usr,
    "password" : sta_pwd,
    "version": sta_ver,
    "bundleIdentifier": sta_bid
  }
}

sta_s_alter_list = {'line1': 'Address Line 1', 'id': 'Given ID', 'name': 'Site Name'}
sta_s_keep_list = ['Address Line 1', 'Format', 'Company', 'Country', 'County', 'Date', 'Given ID', 'Latitude', 'Longitude', 'Other Town', 'Postcode', 'Site Name', 'Site Number', 'Status', 'Telephone Number', 'Town']

sta_del_list = ['id', 'choices', 'portion_name', 'salesAreaId', 'supplementPrice']
sta_alter_list = {"price": "Price", "name": "Portion Size"}