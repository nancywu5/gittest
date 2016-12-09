
# coding: utf-8

# Author: WU Nan
# Date: Dec 8th

from __future__ import print_function
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys
from operator import add
from pyspark import SparkContext
from pyspark.mllib.regression import LinearRegressionWithSGD, LabeledPoint
from pyspark.mllib.classification import SVMWithSGD, SVMModel

import numpy as np
import csv
import math

#period: the days of period you choose
#✅WELL DONE
def get_csv_data(filename):
    filename = "file:/Users/nancywu/sparkhadoop/datatest/"+filename+".csv"
    File = sc.textFile(filename)
    File.map(lambda line: line.split(","))
    File.filter(lambda line: len(line) > 0)
    File.map(lambda line: (line[0], line[1]))

    data = File.collect()
    stock_text = [d.split(",") for d in data]

    #start date = 2016.4.1, means predict until this date and end, 
    #period= training period time= 90 days before predict day ,=n , n path which is in simulation program fucntion
    start, period = 1, 90
    training_period =90

    open_price_train = [float(stock_text[i][1]) for i in range(period+training_period+1 ,period+1, -1)]
    close_price_train = [float(stock_text[i][4]) for i in range(period+training_period+1 ,period+1,-1)]
    
    #相对于true_price前一天的数据集
    open_price = [float(stock_text[i][1]) for i in range (start+period,start,-1)]
    close_price = [float(stock_text[i][4]) for i in range (start+period,start,-1)]
    
    Date=["Date"]
    [Date.append(stock_text[i][0]) for i in range(start + period-1,start,-1)]
    
    True_price =True_price_train= ["True_price"]
    [True_price_train.append((float(stock_text[j][1])+float(stock_text[j][4]))*0.5) for j in range(period+training_period ,period, -1)]
    [True_price.append((float(stock_text[j][1])+float(stock_text[j][4]))*0.5) for j in range(start+period-1,start,-1)]
    print ("get_csv_data: done")
    #print ( open_price,close_price,S0,True_price)
    return open_price, close_price, open_price_train, close_price_train, True_price, True_price_train, Date


#NUMPY WRONG!!solved it already on Nov.28   ✅

def LinearRegression(filename):
    open_price, close_price, open_price_train, close_price_train, True_price,True_price_train, Date = get_csv_data(filename)
    output=[]
    for i in range(1,len(Date)):
        #features=训练集,这里可以自己调整去做尝试； label=target目标值
        #true_price_train与open_price, close_price一样都是前面90天的数据。trainin_period数据集
        #tmp = LabeledPoint(label=True_price_train[i],features=[open_price_train[i],close_price_train[i]])
        #tmp = LabeledPoint(label=True_price_train[i],features=[open_price_train[i]])
        ############
        ###########
        ##########需要修改
        tmp = LabeledPoint(label=True_price_train[i],features=[close_price_train[i]])
        output.append(tmp)
    output_train_data=sc.parallelize(output)                                                        
    output_model=LinearRegressionWithSGD.train(output_train_data,step=0.001,iterations=100000)
    return Date, True_price, output_model,open_price, close_price

def generation_output(Date, True_price,output_model,open_price, close_price):
    #Date=
    output = [["Date","Trueprice","Predictprice_upperbound","Predictprice_lowerbound"]]
    for i in range (1,len(Date)):
        S1=output_model.predict([open_price[i]])
        S2=output_model.predict([close_price[i]])
        tmp = [Date[i],True_price[i],S1,S2]
        output.append(tmp)
    return output
   

#Well done
def writeToElastic(fileindex,es,filename,stock_text):
    df=stock_text
    j = 1
    actions = []
    count = int(len(df))
    while (j < count):
        action = {
                   "_index": fileindex, # 这里不可以是大写，都是小写
                   "_type": filename,
                   "_id": j,
                   "_source": {
                               "date":df[j][0],
                               "trueprice":float(df[j][1]),
                               "predictprice_upper":float(df[j][2]),
                               "predictprice_lower":float(df[j][3]),
                               #"timestamp": datetime.now()
                                }
                   }
        print(action)
        actions.append(action)
        j += 1
        if (len(actions) == 180):
            helpers.bulk(es, actions)
            del actions[0:len(actions)]
            
    if (len(actions) >0 ):
            helpers.bulk(es, actions)
            del actions[0:len(actions)]
    
    
        
if __name__ =="__main__":
    sc = SparkContext(appName="Monte Carlo")
    Ticker = sc.textFile("file:/Users/nancywu/sparkhadoop/datatest/Tickertest.csv")
    filelist = Ticker.map(lambda f: f.split(",")).collect()
    #l = Ticker.collect()
    #filelist = l[0].split(",")
    print(filelist)
    es = Elasticsearch()
    print("===========start============")
    for f in filelist:
        try:
            name = f[0]+".csv"
            print (name,"LinearRegression",f[0])
            Date, True_price, output_model, open_price, close_price = LinearRegression(f[0])
            print("=====LinearRegression pass=====")
            output = generation_output(Date, True_price,output_model,open_price, close_price)
            #9.25是今天的stock，预测的是t+1. 训练的时候进入的是t+1的值。则出来的是t+1; 👆训练的3个，所以这里是【10，6，7】
            #这里是3个features
            #print (output.predict([10,6,7]))
            print("=======output pass=======")
            print (output)
            sc.parallelize(output).repartition(1).saveAsTextFile("file:/Users/nancywu/sparkhadoop/datatest_result/" + name)
            writeToElastic("linear",es,name,output)
        except:
            import traceback
            traceback.print_exc()
            print("No service for this stock")





