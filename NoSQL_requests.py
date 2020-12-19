# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 10:21:54 2020

@author: mattd
"""

import pymongo
from bson.raw_bson import RawBSONDocument
import time

def getLowerPrice(): # print lower price of bitcoin registered in database
   myclient = pymongo.MongoClient("mongodb://localhost:27017/",document_class=RawBSONDocument).samples
   mydb = myclient["CoinBase"]
   mycol = mydb["Candles"]
   myquery = mydb.mycol.find().sort([("low",pymongo.DESCENDING)])
   print(myquery.limit(1)[0]["low"])
 
#getLowerPrice()

def getOpenPriceOver6000(): #print ids list of candles with open price over 6000$
   myclient = pymongo.MongoClient("mongodb://localhost:27017/",document_class=RawBSONDocument).samples
   mydb = myclient["CoinBase"]
   mycol = mydb["Candles"]
   myquery = mydb.mycol.find({"open" : {"$gt" : "6000"}})
   for i in range (50):
           print(myquery[i]["_id"])
           
#getOpenPriceOver6000()

def getCandlesOnLastHour(): # print ids list of candles on last hour
   myclient = pymongo.MongoClient("mongodb://localhost:27017/",document_class=RawBSONDocument).samples
   mydb = myclient["CoinBase"]
   mycol = mydb["Candles"] 
   now = int(time.time())
   oneHour = 60*60
   myquery = mydb.mycol.find({"time" : {"$gt" : ('"'+str(now - oneHour)+'"')}})
   for i in range (20):
           print(myquery[i]["_id"])
           print(i)
   
NB_MINUTES_POLLING = 2 # we do the polling on 2 minutes but it can be changed
         
def menu():
        print("1 : get lower bitcoin price registered in database")
        print("2: get ids of candles with open price over 6000$")
        print("3: get candles on last 15 minutes")
        choix = int(input("your choice : "))
        if(choix == 1):
                i = 0
                while i < NB_MINUTES_POLLING:
                        if (int(time.time())%60 == 0):
                                getLowerPrice()
                                i += 1
        if(choix == 2):
                i = 0
                while i < NB_MINUTES_POLLING:
                        if (int(time.time())%60 == 0):
                                getOpenPriceOver6000()
                                i += 1
        if(choix == 3):
                
                i = 0
                while i < NB_MINUTES_POLLING:
                        if (int(time.time())%60 == 0):
                                getCandlesOnLastHour()
                                i += 1
                
menu()