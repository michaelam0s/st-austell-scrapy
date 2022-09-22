import re
import json
import string
import random
from datetime import datetime

import numpy as np
from scrapinghub import ScrapinghubClient
import pandas as pd

def latest_items(shubkey, projectID, spider):
  client=ScrapinghubClient(shubkey)
  project=client.get_project(projectID) #client.projects.list()
  jobs=project.jobs.list(spider=spider, state="finished")[0]
  return client.get_job(jobs['key'])

def metadate_items(job):
  ts = int(job.metadata.get('finished_time'))
  return datetime.utcfromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')

def id_generator(size=6, chars=string.ascii_lowercase + string.digits):
  return ''.join(random.choice(chars) for _ in range(size))

def user_generator():
  userDeviceIdentifier=id_generator(7)+"-"+id_generator(4)+"-"+id_generator(4)+"-"+id_generator(12)
  return userDeviceIdentifier

def obj_to_json(key,response):
  js = re.findall("var {0} =(.+?);\n".format(key), response.text, re.S)[0]
  js=js.replace("\n", "")
  js=js.replace("\\x2D", "-")
  js=js.replace("\\x20", " ")
  js = js.replace("\\x27", "")
  js=" ".join(js.split())
  js=re.sub("((?=\D)\w+):", r'"\1":',js)
  js = js.replace("} },", "} } }").split('''"aslinw''', 1)[0]
  return json.loads(js)

def delete_items(json, alter):
  for i in alter:
    for j in list(json):
      if i == j:
        del json[f'{j}']
  return json

def rename_items(json, alter):
  for i in list(json):
    for j in alter:
      if i == j:
        alt_name = alter[f'{i}']
        json[f'{alt_name}'] = json[i]
        del json[i]
  return json

def keep_items(json, alter):
  for element in list(json):
    if element not in alter:
      del json[element]
  return json


def Key_Product_Matching(Companies, Products):

  Products_List = Products[f'{Companies} ID'].tolist()
  Products_List = [x for x in Products_List if x]

  yield Products_List

def Key_Products_Mapping(Companies, Products, Product_ID):
  DF = Products.loc[Products[f'{Companies}'] == Product_ID]
  series = DF.iloc[0]
  yield series['Stonegate Product']

def Key_Products_To_Json(Products, Product):
  List = Products.values
  List = List.tolist()
  List = np.array(List)
  List = np.nan_to_num(List).tolist()
  # try:
  lst = [x for x in List if str(Product) in x[1]]
  item = lst[0][0]
  # except:
  #   item = 'N/A'

  return item


