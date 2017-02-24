
# coding: utf-8


#
#
#Author: WU Nan
#Date: 2016.12.5
#Description: This is the csv to csv programme file to 
#Further target: ⚠️
#To make 5 star for a stock on risk assessment: ND:要细分成四档；DD细分成6档。 0.5*6+0.5*4最多是5颗星。
#
#
#
from __future__ import print_function
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys
from operator import add
from pyspark import SparkContext

import numpy as np
import csv
import math
from scipy import stats
import pandas as pd
import scipy


#period: the days of period you choose
#WELL DONE
def get_csv_data(filename):
            filename = "hdfs:///nw/Stock/"+filename+".csv"
            File = sc.textFile(filename)
            File.map(lambda line: line.split(","))
            File.filter(lambda line: len(line) > 0)
            File.map(lambda line: (line[0], line[1]))
            data = File.collect()
            stock_text = [d.split(",") for d in data]
            #From backward of Period of present date (90 days before 2016.4.1) to present date 2016.4.1
            #historical period you want to choose
            period = 360*3
            
            #1:openprice
            #2:highprice
            #3:lowprice
            #4:closeprice
            #5:volume
            #6:Adjcloseprice
            if (len(stock_text)<period): period=len(stock_text)-1
            open_price = [round(float(stock_text[i][1]),10) for i in range(period,0,-1)]
            close_price = [round(float(stock_text[i][4]),10) for i in range(period,0,-1)]
            volume=[long(stock_text[i][5]) for i in range (period, 0,-1)]
            date=["Date"]
            [date.append(stock_text[i][0]) for i in range(period,0,-1)]
            print ("get_csv_data: done")
            return open_price,close_price,volume,date


def normal_distribution_test(open_price,close_price):
        ##risk distance round精度调整
            risk_assessment_initial = list(map(lambda x: round(float(x[0]-x[1]),2), zip(open_price, close_price)))
            ##
            print("====risk assessment initial======")
            tmp=sc.parallelize(risk_assessment_initial)
            Change_initial = tmp.map(lambda distance: (distance, 1)).reduceByKey(lambda a, b: a + b)
            Change_list=Change_initial.collect()
            distance_list=[]
            frequency_list=[]
            for (distance,frequency) in Change_list:
            #这里限制了波动噪声。为ND_test做准备
                if (frequency<len(risk_assessment_initial)*0.5):
                    distance_list.append(distance)
                    frequency_list.append(frequency)
            #3 methods of statistics to test how distribution it is.
            #Make risk range for particular stock
            x = frequency_list
            signal=1
            if (len(x)<=3): return signal, Change_list
            
            shapiro_results = scipy.stats.shapiro(x)
            a1=shapiro_results[0]
            a2=round(shapiro_results[1],8)
            matrix_sw = [['', 'DF', 'Test Statistic_SW', 'p-value'],['Sample Data', len(x) - 1, a1, a2]]
            print(matrix_sw)
            #shapiro_result more close to 1 more probabily of normal distribution it is
            #####
            ks_results = scipy.stats.kstest(x, cdf='norm')
            b1=ks_results[0]
            b2=round(ks_results[1],8)
            matrix_ks = [['', 'DF', 'Test Statistic_KS', 'p-value'],['Sample Data', len(x) - 1, b1, b2]]
            print(matrix_ks)
            ###
            dagostino_results = scipy.stats.mstats.normaltest(x)
            c1=dagostino_results[0]
            c2=round(dagostino_results[1],8)
            matrix_dp = [['', 'DF', 'Test Statistic_dp', 'p-value'],['Sample Data', len(x) - 1, c1, c2]]
            print(matrix_dp)
            ###########risk_ND_signal##############
            if (abs(a1-a2)>0.8 and (abs(b1-b2)>0.8 and b2<0.0005) and (abs(c1-c2)>0.8 and c2<0.0005)):
                #normal distribution. low risk
                signal=5
            elif ((abs(a1-a2)>0.8 and (abs(b1-b2)>0.8 and b2<0.0005))or (abs(a1-a2)>0.8 and (abs(c1-c2)>0.8 and c2<0.0005)) or ((abs(b1-b2)>0.8 and b2<0.0005) and (abs(c1-c2)>0.8 and c2<0.0005))):
                signal=4
            elif (abs(a1-a2)>1 or abs(b1-b2)>1 or abs(c1-c2)>1): 
                signal=3
            else:
                #not normal distribution. high risk
                signal =2
            return signal,Change_list
        
#Max_drawdown   
#回撤结束时间点
#close_price是需要探测的最大回撤率的时间范围内的价格区间，close_price=[price1,price2,price3],时间标度:day_i,day_i+1
# 回撤开始的时间点
def max_drawdown_test(close_price):
        #dd:drawdown
        risk_DD_signal=1
        if (len(close_price)<=3): return risk_DD_signal, 0
        i = np.argmax(np.maximum.accumulate(close_price) - close_price)
        j = np.argmax(close_price[:i])
        dd_return =(float(close_price[i]) /close_price[j]) - 1
        ####回撤signal_setting##########
        if (abs(dd_return)<0.05):
            risk_DD_signal=5
        elif (abs (dd_return)<0.1):
            risk_DD_signal=4
        elif (abs (dd_return)<0.3):
            risk_DD_signal=3
        elif (abs (dd_return)<0.5):
            risk_DD_signal=2
        elif (abs (dd_return)<0.8):
            risk_DD_signal=1
        else:
            risk_DD_signal=0
        return risk_DD_signal, dd_return

#Value at Risk
#Example:With 99% confidence, we expect that the worst daily loss will not exceed 8.2%.
#Or, if we invest $100, we are 99% confident that our worst daily loss will not exceed $8.2.
#Read more:
def historicalVaR(close_price,confidenceLevel):
    returnRate=[]
    [returnRate.append((float(close_price[j+1])-float(close_price[j]))/float(close_price[j])) for j in range(0,len(close_price)-1,1)]
    n=len(returnRate)
    m=int(n*(1-confidenceLevel))
    returnRate.sort()
    print (returnRate,n)
    result=returnRate[m]
    return result    
    

def risk_assessment(filename):
    """
        filename=stock_ticker
    """
    open_price,close_price, volume, date= get_csv_data(filename)
    risk_ND_signal, Change_list=normal_distribution_test(open_price,close_price)
    risk_DD_signal, dd_return = max_drawdown_test(close_price)
    #risk_VaR_signal, result=
    print (filename, "dd_return is:",dd_return)
    return Change_list,risk_DD_signal, risk_ND_signal

def writeToElastic(fileindex,es,filename,risk_ND_text,risk_rate):
    #filename=stock_ticker
    df=risk_ND_text
    j = 1
    actions = []
    count = int(len(df))
    while (j < count):
        action = {
                   "_index": fileindex, # 这里不可以是大写都是小写
                   "_type": filename,
                   "_id": j,
                   "_source": {
                               "pricechange":df[j][0],
                               "frequency":float(df[j][1]),
                               "riskrate":risk_rate
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
            
            
def CalRiskRate(stock_ticker):
    #####Risk rate preparation
    ####Risk level set up by Customer set 
    ###5:100%; 4:80%; 3:60%; 2,1,0
    risk_w_ND=0.5
    risk_w_DD=0.5
    #risk_w_VaR=0.3
    ######risk_w_ND+risk_w_DD+...+other risk_w_assess=100%
    Change_list_Normal_Distribution, risk_DD_signal, risk_ND_signal = risk_assessment(stock_ticker)
    risk_rate=risk_DD_signal*risk_w_DD  + risk_ND_signal*risk_w_ND
    return risk_rate
    
    
    
    
if __name__ =="__main__":
    #sc = SparkContext(appName="Monte Carlo")
    Ticker = sc.textFile("hdfs:///nw/Ticker.csv")
    filelist = Ticker.map(lambda f: f.split(",")).collect()
    print(filelist)
    es = Elasticsearch(['http://10.1.2.3:9200'])
    #####Risk rate preparation
    ####Risk level set up by Customer set 
    ###5:100%; 4:80%; 3:60%; 2,1,0
    risk_w_ND=0.5
    risk_w_DD=0.5
    risk_list=[]
    ######risk_w_ND+risk_w_DD+...+other risk_w_assess=100%
    ###############################################################
    print("===========start============")
    for f in filelist:
        try:
            name = f[0]+".csv"
            print (name,"simulation",f[0])
            Change_list_Normal_Distribution, risk_DD_signal, risk_ND_signal = risk_assessment(f[0])
            print("=====risk assessment pass=====")
            risk_rate=risk_DD_signal*risk_w_DD  + risk_ND_signal*risk_w_ND
            #risk_rate=CalRiskRate(f[0])
            ###########risk_rate Output#######################
            risk_list.append([f[0],risk_rate])
            print(f[0],"risk_rate:",risk_rate)
            ########
            print("=======output pass=======")
            writeToElastic("riskfreq",es,name,Change_list_Normal_Distribution,risk_rate)
            print("=======ES pass=======")
        except:
            import traceback
            traceback.print_exc()
            print("No service for this stock")






