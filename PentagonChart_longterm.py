
#
#Author: WU Nan
#Date: 2017.02.22
#Description: This is the csv to csv programme file 
#It calculates the pentagon chart five elements numbers for each stock.
#
#LONG TERM VERSION
#
"""
  input: ticker.csv  
  output: 写入es所有计算的和并行读取的stock all detailed informations
  
  用途：为后续UI读取es的数据，draw chart of pentagon with 5 elements 做准备
  **只用了treeRDD, 在cluster上可以尝试其他的RDD并行处理，选取最优解
"""
from __future__ import print_function
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys
from operator import add
from pyspark import SparkContext
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.regression import LinearRegressionModel, LinearRegressionWithSGD
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.regression import IsotonicRegression, IsotonicRegressionModel
from pyspark.mllib.regression import LabeledPoint


import numpy as np
import csv
import math
from scipy import stats
import pandas as pd
import scipy


##########---------get_csv_data------------###################
def get_csv_data(filename):
            filename = "hdfs:///nwu/Stock/"+filename+".csv"
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
            return open_price,close_price,volume,date

def get_csv_data_30(filename):
            filename = "hdfs:///nwu/Stock/"+filename+".csv"
            File = sc.textFile(filename)
            File.map(lambda line: line.split(","))
            File.filter(lambda line: len(line) > 0)
            File.map(lambda line: (line[0], line[1]))
            data = File.collect()
            stock_text = [d.split(",") for d in data]
            #From backward of Period of present date (90 days before 2016.4.1) to present date 2016.4.1
            #historical period you want to choose
            period = 30
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
            return open_price,close_price,volume,date
        
def get_csv_data_90(filename):
            filename = "hdfs:///nwu/Stock/"+filename+".csv"
            File = sc.textFile(filename)
            File.map(lambda line: line.split(","))
            File.filter(lambda line: len(line) > 0)
            File.map(lambda line: (line[0], line[1]))
            data = File.collect()
            stock_text = [d.split(",") for d in data]
            #From backward of Period of present date (90 days before 2016.4.1) to present date 2016.4.1
            #historical period you want to choose
            period = 90
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
            return open_price,close_price,volume,date
        
def get_csv_data_360(filename):
            filename = "hdfs:///nwu/Stock/"+filename+".csv"
            File = sc.textFile(filename)
            File.map(lambda line: line.split(","))
            File.filter(lambda line: len(line) > 0)
            File.map(lambda line: (line[0], line[1]))
            data = File.collect()
            stock_text = [d.split(",") for d in data]
            #From backward of Period of present date (90 days before 2016.4.1) to present date 2016.4.1
            #historical period you want to choose
            period = 360
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
            return open_price,close_price,volume,date
        
##################-------------------History------------------------------########################
def CalHistory(stock_ticker):
    open_price,close_price, volume,date= get_csv_data(stock_ticker)
    if (len(close_price)>=3):
        startday=close_price[1]
        endday=close_price[-1]
        histrend=(endday-startday)/startday
        #print ("startday_price_T-360:", startday)
        #print ("endday_price_T0:",endday)
        print ("histrend",histrend)
        if (histrend<-0.8):
            his=1.0
        elif (histrend<-0.1 and histrend>-0.8):
            his=2.0
        elif (histrend>-0.1 and histrend <0.01):
            his=3.0
        elif (histrend>0.01 and histrend <0.2):
            his =4.0
        else:
            his=5.0
        return his
    else:
        his=2.5
        return his
        

##################-------------------Future----------------------------###########################
def FuturePredict(stock_ticker):
    #Use Regression model Linear Regression Model do predict
    open_price_train, close_price_train, volume, date= get_csv_data_360(stock_ticker)
    output=[]
    for i in range(1,len(date)-2):
        #这里用每天的open price作为target 用close price 作为训练features特征
        #⚠️此处应该要改，这里先试验
        tmp = LabeledPoint(label=close_price_train[i+1],features=[open_price_train[i]])
        output.append(tmp)
        
    output_train_RDD=sc.parallelize(output).cache()                                                          
    #lrm=LinearRegressionWithSGD.train(output_train_RDD,step=0.001,iterations=100000)
    tree = DecisionTree.trainRegressor(output_train_RDD, categoricalFeaturesInfo={},impurity='variance', maxDepth=5, maxBins=30)
    #forest = RandomForest.trainRegressor(output_train_RDD, categoricalFeaturesInfo={}, numTrees=3, featureSubsetStrategy="auto", impurity='variance', maxDepth=5, maxBins=30)
    #gradient = GradientBoostedTrees.trainRegressor(output_train_RDD, categoricalFeaturesInfo={}, numIterations=10)
    print ("\n============MODEL Evaluation=============\n")
    #es_modelname=['lrm','tree','forest','gradient']
    x=0
    err=1000
    #此处选择最优rdd 做predict
    output_model_RDD=tree
    
    """
    if not tree:
        output_model_RDD=lrm
    """
    
    """
    for model in [lrm, tree, forest, gradient]:
        predictions = model.predict(output_train_RDD.map(lambda x: x.features))
        labelsAndPredictions = output_train_RDD.map(lambda lp: lp.label).zip(predictions)
        MSE = (labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() /float(output_train_RDD.count()))**0.5
        if (err>MSE):
            err=MSE
            output_model_RDD=model
            #output_model=es_modelname[x]
        x+=1
    print("output_model:",output_model)
    #output_model_RDD=tree
    """
    train_len=len(close_price_train)
    output=[]
    S1=output_model_RDD.predict([open_price_train[train_len-1]])
    #predictlist = [["startday"+date[train_len],"Predictprice"]]
    #tmpdate=1
    long_period=90
    for i in range (long_period+1,1,-1):
        #tmp = [str(tmpdate)+"day",S1]
        output.append(S1)
        #predictlist.append(tmp)
        #此处是一个短的周期，其保证比如short period=30days, 此predict在上一个30天的循环周期，保证波动一致性
        S0=(S1+close_price_train[i+long_period])*0.5
        S1=output_model_RDD.predict([S0])
        #tmpdate+=1
    #print ("\n============PredictList=============\n")
    #print (predictlist)
    return output

def CalFuture(stock_ticker): #待修改
    Futurelist=FuturePredict(stock_ticker)
    #print ("\n============FutureList=============\n")
    #print(Futurelist)
    startday=Futurelist[0]
    endday=Futurelist[-1]
    
    futurerend=(endday-startday)/startday
    print ("startday_price_T-360:", startday)
    print ("endday_price_T0:",endday)
    print ("futurerend",futurerend)
    if (futurerend<-0.8):
        future=1.0
    elif (futurerend<-0.6 and futurerend>-0.8):
        future=2.0
    elif (futurerend>-0.6 and futurerend <0.6):
        future=3.0
    elif (futurerend>0.6 and futurerend <0.7):
        future=4.0
    else:
        future=5.0
    return future
    
    
##################-------------------Health------------------------------########################
"""Health Cal START"""
def normal_distribution_test(open_price,close_price):
        ##risk distance round精度调整
            risk_assessment_initial = list(map(lambda x: round(float(x[0]-x[1]),2), zip(open_price, close_price)))
            ##
            #print("====risk assessment initial======")
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
            signal=1.5
            if (len(x)<=20): return signal, Change_list
            
            shapiro_results = scipy.stats.shapiro(x)
            a1=shapiro_results[0]
            a2=round(shapiro_results[1],8)
            #matrix_sw = [['', 'DF', 'Test Statistic_SW', 'p-value'],['Sample Data', len(x) - 1, a1, a2]]
            #print(matrix_sw)
            #shapiro_result more close to 1 more probabily of normal distribution it is
            #####
            ks_results = scipy.stats.kstest(x, cdf='norm')
            b1=ks_results[0]
            b2=round(ks_results[1],8)
            #matrix_ks = [['', 'DF', 'Test Statistic_KS', 'p-value'],['Sample Data', len(x) - 1, b1, b2]]
            #print(matrix_ks)
            ###
            dagostino_results = scipy.stats.mstats.normaltest(x)
            c1=dagostino_results[0]
            c2=round(dagostino_results[1],8)
            #matrix_dp = [['', 'DF', 'Test Statistic_dp', 'p-value'],['Sample Data', len(x) - 1, c1, c2]]
            #print(matrix_dp)
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
        risk_DD_signal=2.5
        if (len(close_price)<=20): return risk_DD_signal, 0
        i = np.argmax(np.maximum.accumulate(close_price) - close_price)
        j = np.argmax(close_price[:i])
        dd_return =(float(close_price[i]) /close_price[j]) - 1
        ####回撤signal_setting##########
        if (abs(dd_return)<0.05):
            risk_DD_signal=5.0
        elif (abs (dd_return)<0.1):
            risk_DD_signal=4.0
        elif (abs (dd_return)<0.3):
            risk_DD_signal=3.0
        elif (abs (dd_return)<0.5):
            risk_DD_signal=2.0
        elif (abs (dd_return)<0.8):
            risk_DD_signal=1.0
        else:
            risk_DD_signal=0
        return risk_DD_signal, dd_return

#Value at Risk
#Example:With 99% confidence, we expect that the worst daily loss will not exceed 8.2%.
#Or, if we invest $100, we are 99% confident that our worst daily loss will not exceed $8.2.
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
    ###此处可以优化
    open_price,close_price, volume, date= get_csv_data(f[0])
    risk_ND_signal, Change_list=normal_distribution_test(open_price[1:],close_price[1:])
    risk_DD_signal, dd_return = max_drawdown_test(close_price[1:])
    #risk_VaR_signal, result=
    print (filename, "dd_return is:",dd_return)
    return Change_list,risk_DD_signal, risk_ND_signal

                    
def CalRiskRate(stock_ticker):
    #####Risk rate preparation
    ####Risk level set up by Customer set 
    ###5:100%; 4:80%; 3:60%; 2,1,0
    #risk_w_VaR=0.3
    ######risk_w_ND+risk_w_DD+...+other risk_w_assess=100%
    risk_w_ND=0.5
    risk_w_DD=0.5
    Change_list_Normal_Distribution, risk_DD_signal, risk_ND_signal = risk_assessment(stock_ticker)
    risk_rate=risk_DD_signal*risk_w_DD + risk_ND_signal*risk_w_ND
    return risk_rate
"""Health Cal END"""
##################--------------------Popularity-----------------------------############################

"""Popularity Cal START"""
def CalPop(stock_ticker):
    ###此处可以优化
    open_price,close_price, volume, date= get_csv_data(stock_ticker)
    avg=np.average(volume[1:])
    if (avg<50):
        pop=0.0
    elif (avg>50 and avg<500):
        pop=1.0
    elif (avg>500 and avg<50000):
        pop=2.0
    elif (avg >50000 and avg <500000):
        pop=3.0
    elif (avg>500000 and avg<50000000):
        pop=4.0
    else:
        pop=5.0
    return pop

"""Popularity Cal END"""
##################------------------------Return-------------------------############################
"""Return Cal START"""
#short_term = 90days  long_term=1-2years
#max_dd:最大回撤率
#max_gain:最大回报率
def CalReturn(stock_ticker):
    open_price,close_price, volume, date= get_csv_data_360(stock_ticker)
    r=1.0
    close_price=close_price[1:]
    i = np.argmax(np.maximum.accumulate(close_price) - close_price)
    ii=np.argmax(close_price-np.minimum.accumulate(close_price))
    if (len(close_price)>7 and close_price[:i]):#7days 1 week
        j = np.argmax(close_price[:i])
        jj= np.argmin(close_price[:i])
        max_dd =(float(close_price[i]) /close_price[j]) - 1
        max_gain =(float(close_price[ii]) /close_price[jj]) - 1
        print (max_dd,max_gain)
        if (max_dd>-0.1 and max_gain>0.3):
                r=5.0
        elif (max_dd>-0.2 and max_gain>0.1):
                r=4.0
        elif (max_dd>-0.3 and max_gain>0.05):
                r=3.0
        elif (max_dd>-0.5 or max_gain>0.01):
                r=2.0
        else:
                r=1.0
    return r

"""Return Cal END"""

""" 股票详细资料字典
"""
def StockDict():
    stockdict= {}
    File = sc.textFile("hdfs:///nw/stockdetails_all.csv")
    File.map(lambda line: line.split(","))
    File.filter(lambda line: len(line) > 0)
    File.map(lambda line: (line[0], line[1]))
    data = File.collect()
    dic = [d.split(",") for d in data]
    #print (dic)
    for i in dic:
        stockdict[i[0]] = [i[1:]]
    return stockdict

"""Write Details of Stock into Elasticsearch
   with 5 elements of each pentagon stock chart as well
"""
def writeToElastic(fileindex,es,stockdict,StockPlist):
    #filename=stock_ticker
    df=StockPlist
    j = 0
    actions = []
    count = int(len(df))
    while (j < count):
        tmplist=stockdict.get(df[j][0])
        if tmplist:
            action = {
                       "_index": fileindex, # 这里不可以是大写都是小写
                       "_type": df[j][0],
                       "_id": j,
                       "_source": {
                                   "ticker":df[j][0],
                                   "details":tmplist[0],
                                   "pentagon":df[j][1],
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
    sc = SparkContext(appName="LongTerm Pentagon")
    #short term= 90days  long term= 360days 
    Ticker = sc.textFile("hdfs:///nw/Ticker_1000.csv")
    filelist = Ticker.map(lambda f: f.split(",")).collect()
    #print(filelist)
    es = Elasticsearch(["http://10.1.2.3:9200"])
    StockPlist=[]
    print("===========start============")
    stockdict=StockDict()
    for f in filelist:
        try:
            tmplist=[f[0]]
            #name=f[0]+"_pentagon.csv"
            #output_predict,modelname = generation_output(f[0])
            """history, future, health(risk), return, popularity"""
            history=CalHistory(f[0])
            future=CalFuture(f[0])
            health=CalRiskRate(f[0])
            popularity=CalPop(f[0])
            r=CalReturn(f[0])
            print ("===========Cal5Elements PASS============")
            tmplist.append([history,future,health,popularity,r])
            StockPlist.append(tmplist)
            #sc.parallelize(output_predict).repartition(1).saveAsTextFile("file:/Users/nancywu/sparkhadoop/datatest_result/" + name)
        except:
            import traceback
            traceback.print_exc()
            print("No service for this stock")
    writeToElastic('stocklongtermd',es,stockdict,StockPlist)
    print("=======ES writing pass=======")
    #print (StockPlist)
            





