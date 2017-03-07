
# coding: utf-8
#Author:NancyWU
#Date: Dec, 13th
#Last Modification: 2017, March 7th
#Function Description: 4 Model building/ Estimate/ Find the most least error to do stock prediction
#Regression models (Spark MLlib):
#   LinearRegression
#	DecisionTree
#	RandomForest
#	GradientBoostedTrees
#


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

#period: the days of period you choose
def get_csv_data(filename):
    filename = "hdfs:///nw/Stock/"+filename+".csv"
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


#features=训练集,这里可以自己调整去做尝试； label=target目标值
#true_price_train与open_price, close_price一样都是前面90天的数据。trainin_period数据集
#tmp = LabeledPoint(label=True_price_train[i],features=[open_price_train[i],close_price_train[i]])
def Regression_Model(filename):
    open_price, close_price, open_price_train, close_price_train, True_price,True_price_train, Date = get_csv_data(filename)
    output=[]
    for i in range(1,len(Date)):
        tmp = LabeledPoint(label=True_price_train[i],features=[close_price_train[i]])
        output.append(tmp)
        
    output_train_RDD=sc.parallelize(output).cache()                                                          
    lrm=LinearRegressionWithSGD.train(output_train_RDD,step=0.001,iterations=100000)
    tree = DecisionTree.trainRegressor(output_train_RDD, categoricalFeaturesInfo={},impurity='variance', maxDepth=5, maxBins=30)
    forest = RandomForest.trainRegressor(output_train_RDD, categoricalFeaturesInfo={}, numTrees=3, featureSubsetStrategy="auto", impurity='variance', maxDepth=5, maxBins=30)
    gradient = GradientBoostedTrees.trainRegressor(output_train_RDD, categoricalFeaturesInfo={}, numIterations=10)
    
    print ("\n============MODEL Evaluation=============\n")
    model_name = ['LinearRegression','DecisionTree','RandomForest','GradientBoostedTrees']
    es_modelname=['lrm','tree','forest','gradient']
    result = ''
    x = 0
    err=1000
    test_model='LinearRegression'
    #此处更换不同的RDD
    output_model_RDD=lrm
    for model in [lrm, tree, forest, gradient]:
        predictions = model.predict(output_train_RDD.map(lambda x: x.features))
        labelsAndPredictions = output_train_RDD.map(lambda lp: lp.label).zip(predictions)
        MSE = (labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() /float(output_train_RDD.count()))**0.5
        #print ("Predictions: ", valuesAndPreds.take(10))
        result += model_name[x] + "\tMean Squared Error\t=" + str(MSE) + "\n"
        if (err>MSE):
            err=MSE
            output_model=model
            es_model=es_modelname[x]
        x += 1
    print (result)
    print (es_model)
    return Date, True_price, output_model_RDD, open_price, close_price, es_model

def generation_output(stock_name):
    Date, True_price, output_model_RDD, open_price, close_price,modelname = Regression_Model(stock_name)
    output = [["Date","Trueprice","Predictprice"]]
    for i in range (1,len(Date)):
        #S1=output_model.predict([open_price[i]])
        S1=output_model_RDD.predict([close_price[i]])
        tmp = [Date[i],True_price[i],S1]
        output.append(tmp)
    return output,modelname
   

#Well done
def writeToElastic(fileindex,modelname,es,filename,stock_text):
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
                               "predictprice":float(df[j][2]),
                               "regressionmodel":str(modelname)
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
    sc = SparkContext(appName="predictmodels")
    Ticker = sc.textFile("hdfs:///nw/Tickerdemo.csv")
    filelist = Ticker.map(lambda f: f.split(",")).collect()
    #l = Ticker.collect()
    #filelist = l[0].split(",")
    print(filelist)
    es = Elasticsearch(["http://10.1.2.3:9200"])
    print("===========start============")
    for f in filelist:
        try:
            name = f[0]+".csv"
            output_predict,modelname = generation_output(f[0])
            print("=======output pass=======")
            #sc.parallelize(output_predict).repartition(1).saveAsTextFile("file:/Users/nancywu/sparkhadoop/datatest_result/" + name)
            writeToElastic('predictmodels',modelname,es,name,output_predict)
            print("=======es writing pass=======")
        except:
            import traceback
            traceback.print_exc()
            print("No service for this stock")
            



