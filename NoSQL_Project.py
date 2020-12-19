# -*- coding: utf-8 -*-
"""
Created on Sat Feb 29 10:42:56 2020

@author: mattd
"""
import requests,json
import pymongo
import bsonjs
from bson.raw_bson import RawBSONDocument
import time

def getCandles(pair = "BTC-USD",duration = 300): # we get the candles on CoinBase API
    data = requests.get("https://api-public.sandbox.pro.coinbase.com/products/"+pair+"/candles?granularity="+str(duration))
    r_json = json.loads(data.text)
    return r_json    # we store data in r_json
        
#getCandles()

def candlesFormat(data): # give to data the format of a dictionnary
        tmp = {}
        tmp["time"] = str(data[0])
        tmp["low"] = str(data[1])
        tmp["high"] = str(data[2])
        tmp["open"] = str(data[3])
        tmp["close"] = str(data[4])
        tmp["volume"] = str(data[5])
        return (tmp)
#print(type(candlesFormat2(getCandles()[0])))            


def docAlreadyPresent(doc): # verify if a tuple is already present or not in the database
   presence = False
   myclient = pymongo.MongoClient("mongodb://localhost:27017/",document_class=RawBSONDocument).samples
   mydb = myclient["CoinBase"]
   mycol = mydb["Candles"]
   myquery = { "time": doc["time"] }
   if(mydb.mycol.find_one(myquery) != None):
           presence = True
   return presence
   


def createDatabase(): # add data to database
   myclient = pymongo.MongoClient("mongodb://localhost:27017/",document_class=RawBSONDocument)#.samples
   mydb = myclient["CoinBase"]
   mycol = mydb["Candles"]  
   for i in getCandles():
           json_record = candlesFormat(i)
           j = json.dumps(json_record)
           raw_bson = bsonjs.loads(j)
           bson_record = RawBSONDocument(raw_bson)
           if docAlreadyPresent(json_record) == False:
                   result = mydb.mycol.insert_one(bson_record)
                   #print(result.acknowledged)

NB_MINUTES_POLLING = 2 # we do the polling on 2 minutes but it can be changed
         
i = 0
while i < NB_MINUTES_POLLING:
        if (int(time.time())%60 == 0): # each minute we check the database and add new data
                createDatabase()
                i += 1
