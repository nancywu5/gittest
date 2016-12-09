
# coding: utf-8

# In[ ]:

from __future__ import print_function
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys
from operator import add
from pyspark import SparkContext

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
    print(stock_text)
    #start date = 2016.4.1, means predict until this date and end, 
    #period= training period time= 90 days before predict day ,=n , n path which is in simulation program fucntion
    start, period = 1, 90
    training_period =90

    open_price = [float(stock_text[i][1]) for i in range(period+training_period ,period, -1)]
    close_price = [float(stock_text[i][4]) for i in range(period+training_period ,period,-1)]
    Date=["Date"]
    [Date.append(stock_text[i][0]) for i in range(start + period-1,start-1,-1)]
    
    S0 = (float(stock_text[period+start][1])+float(stock_text[period+start][4]))*0.5
    #print (open_price) True_Price is Average of Open-Close Price.
    #print ("+++=Date+++++") 
    #print (Date)
    True_price = ["True_price"]
    #print("=====True price=====")
    [True_price.append((float(stock_text[j][1])+float(stock_text[j][4]))*0.5) for j in range(start+period,start-1,-1)]
    #print(True_price)
    print ("get_csv_data: done")
    #print ( open_price,close_price,S0,True_price)
    return open_price,close_price,S0,True_price,Date

#mu: the mean of sample training size
#v:
#NUMPY WRONG!!solved it already on Nov.28   ✅

def simulation(filename):
    open_price,close_price,S0,True_price,Date = get_csv_data(filename)
    
    simulation_openclose_average_list = list(map(lambda x: float(x[0]+x[1])*0.5, zip(open_price, close_price)))
    print("====simulation initial======")
    print (simulation_openclose_average_list[-1],"s0 IS :",S0)
    
    v= np.array(simulation_openclose_average_list).var()
    mu= np.array(simulation_openclose_average_list).mean()
    t = 1.0/365
    #n=period,which is predict period
    n,M = 90,100000
    result = ["Prediction",S0]
    print ("===preparation===v,mu,t=")
    print(v)
    print(mu)
    #print (result)
    
    for i in range(n-1):
        prediction = sc.parallelize(np.random.normal(0,1,M)).map(lambda x: (mu-0.5*v)*t+x*((v)**0.5)*((t)**0.5)).map(lambda x: math.exp(x)).map(lambda x: S0*x)
        result.append(prediction.mean())
        S0=np.array(simulation_openclose_average_list).mean()
        #S0=simulation_openclose_average_list[-1]
        #S0=S0,变化t
        simulation_openclose_average_list=simulation_openclose_average_list[1:]
        simulation_openclose_average_list.append(prediction.mean())
        v= np.array(simulation_openclose_average_list).var()
        mu= (np.array(simulation_openclose_average_list).mean())
        
        #print("=====simulationlist changing========")
        #print (len(simulation_openclose_average_list))
        #print (simulation_openclose_average_list)
        #print ("===preparation===v,mu,t=")
        #print(v)
        #print(mu)
        #print (result)
    print ("==========predictresult pass========")
    return True_price, result, Date

def generation_output(True_price,prediction, Date):
    #Date=
    output = []
    for i in range(len(Date)):
        tmp = [Date[i],prediction[i],True_price[i]]
        output.append(tmp)
    return output

#Do not  test it yet⚠️
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
                               "prediction":float(df[j][1]),
                               "trueprice":float(df[j][2]),
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
    #sc = SparkContext(appName="Monte Carlo")
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
            print (name,"simulation",f[0])
            True_price,prediction,Date = simulation(f[0])
            print("=====simulation pass=====")
            print(len(True_price),len(prediction))
            output = generation_output(True_price,prediction, Date)
            print("=======output pass=======")
            print (output)
            print (name)
            sc.parallelize(output).repartition(1).saveAsTextFile("file:/Users/nancywu/sparkhadoop/datatest_result/" + name)
            writeToElastic("predictvalue2",es,name,output)
        except:
            print("No service for this stock")


# In[ ]:




# In[ ]:




# In[ ]:



