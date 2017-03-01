# -*- coding: utf-8 -*-
from __future__ import print_function
from django.shortcuts import render,render_to_response
from django.db import models
from django.http import HttpResponseRedirect,HttpResponse
from elasticsearch import Elasticsearch

es =Elasticsearch (["127.0.0.1:9200"])
stockdict={}


import csv
import pandas as pd
import numpy as np
import sklearn.preprocessing
import math

from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
##similarity recommendation
##从价格绝对值，趋势相对值，来算一个时间段内，表现相似度极高的股票。similarity的值
##从选择的股票去推荐相似的股票。
###这个是：target值 每天对比差值
###end of date = 2016.4.1 for testdata

Target_Ticker=" "
global similarity_length
global target
global reocommend_number


def get_df_close_data(filename):
    if (filename == Target_Ticker):
        path = "file:/Users/nancywu/sparkhadoop/datatest/Stock/"+Target_Ticker+".csv"
        File = sc.textFile(path)
        File.map(lambda line: line.split(","))
        File.filter(lambda line: len(line) > 0)
        File.map(lambda line: (line[0], line[1]))
        
        data = File.collect()
        stock_text = [d.split(",") for d in data]
        target_tmp=[]
        target_tmp=pd.DataFrame(target_tmp,columns=['ticker', 'date', 'closePrice','MAprice'])
        target_tmp['date']=[(stock_text[i][0]) for i in range(similarity_length,0,-1)]
        target_tmp['closePrice']=[float(stock_text[i][4]) for i in range (similarity_length,0,-1)]
        
        d=float(stock_text[similarity_length+1][4])
        target_tmp['ratio'] = target_tmp.closePrice /d  - 1
        target_tmp['ticker']=filename
        target_tmp['MAprice']=np.mean(list(target_tmp['closePrice']))
        
        return target_tmp[['ticker', 'date', 'closePrice','ratio','MAprice']]
    else:
        path = "file:/Users/nancywu/sparkhadoop/datatest/Stock/"+filename+".csv"
        File = sc.textFile(path)
        File.map(lambda line: line.split(","))
        File.filter(lambda line: len(line) > 0)
        File.map(lambda line: (line[0], line[1]))
        
        data = File.collect()
        stock_text = [d.split(",") for d in data]
        df=[]
        df=pd.DataFrame(df,columns=['ticker', 'date', 'closePrice', 'ratio','MAprice'])
        df['date']=[(stock_text[i][0]) for i in range(similarity_length,0,-1)]
        df['closePrice']=[float(stock_text[i][4]) for i in range (similarity_length,0,-1)]
        
        d=float(stock_text[similarity_length+1][4])
        df['ratio'] = df.closePrice / d - 1
        df['ticker']=filename
        change=np.mean(list(df['closePrice']))
        df['MAprice']=abs(change-target.MAprice[1])
        
        return df[['ticker', 'date', 'closePrice', 'ratio','MAprice']]

#target是Target_Ticket的closePrice
#### 相似度算法
def cal_minute_bar_similarity(line_data):
    """计算相似度
        
        line_data format: df[['ticker', 'date', 'closePrice', 'ratio','MAprice']] e.g.ticker='AA'
        
        指标：
        1. 偏离值绝对值
        2. 偏离值方差
        3. 偏离值绝对值 - 归一化后
        4. 偏离值方差 - 归一化后
        5. Moving Average Price
        
        Return:
        ['ticker', diff_square,diff_var, diff_square_normalize,diff_var_normalized]
        ----------------------------------------
        square diff and var diff of two lines.
        [diff_square, diff_var]
        [diff_square_normalized, diff_var_normalized]
        """
    tmp = pd.DataFrame()
    #sklearn进行数据预处理 —— 归一化/标准化/正则化
    import sklearn.preprocessing
    scaler =sklearn.preprocessing.MinMaxScaler()
    ticker=line_data.ticker[1]
    ma=line_data.MAprice[1]
    tmp['first'] = target['ratio']
    tmp['second'] = line_data['ratio']
    
    _first, _second = list(target['ratio']), list(line_data['ratio'])
    tmp['first_normalized'] = list(scaler.fit_transform(np.array(_first)))
    tmp['second_normalized'] = list(scaler.fit_transform(np.array(_second)))
    
    tmp['diff'] = tmp['first'] - tmp['second']
    tmp['diff_normalized'] = tmp['first_normalized'] - tmp['second_normalized']
    
    diff_square = sum(tmp['diff'] ** 2)
    diff_square_normalized = sum(tmp['diff_normalized'] ** 2)
    
    diff_var = float(tmp['diff'].var())
    diff_var_normalized = float(tmp['diff_normalized'].var())
    res_square = [round(diff_square, 5), round(diff_square_normalized, 5)]
    res_var = [round(diff_var, 5), round(diff_var_normalized, 5)]
    result=[ticker]+res_square+res_var+[ma]
    return result


# ### 武器库
def build_Srecommend_stock_report_all(res):
    """构造相似度报表
        """
    print ("=================res_df_report start=====================")
    res_df = pd.DataFrame(res,columns=[u'ticker', u'差值平方', u'归一化后差值平方', u'方差', u'归一化后方差','MAprice'])
    print (res_df)
    print ("=================res_df_report pass=====================")
    return res_df[[u'ticker', u'差值平方', u'归一化后差值平方', u'方差', u'归一化后方差','MAprice']]


def get_Srecommend_stock_list(similarity, number):
    """获取最相似的stock
        """
    """此处用差值平方做test-similarity;ascending从小到大
        """
    ##前50 （一级顺序）
    df = pd.DataFrame()
    similary_1 = pd.DataFrame()
    df = similarity.sort(columns=[u'差值平方'], ascending=True)
    similary_1 = df[ : number]
    ##前30 （二级顺序）
    number=10
    df=pd.DataFrame()
    df = similary_1.sort(columns=[u'MAprice'], ascending=True)
    most_similary_list = list(df[ : number][u'ticker'])
    
    return most_similary_list

def Similarity_Rec():
    ### Initiate the case
    rdd_ticker = sc.textFile("file:/Users/nancywu/sparkhadoop/datatest/Ticker_50.csv")
    tickerlist = rdd_ticker.map(lambda f: f.split(",")).collect()
    tickerlist.remove([Target_Ticker])
    tmp_similarity=[]
    ### do the calculation
    for f in tickerlist:
        try:
            name = f[0]+".csv"
            df = get_df_close_data(f[0])
            tmp_similarity.append(cal_minute_bar_similarity(df))
        except:
            import traceback
            traceback.print_exc()
            print("No service for this stock")
    print("=============res_df all cal done===============")
    res_df= build_Srecommend_stock_report_all(tmp_similarity)
    print("=============whole stock list recommend report_pass===============")
    Srecommend_list = get_Srecommend_stock_list(res_df, recommend_number)
    print ("==========Intereted Stock=================")
    print (Target_Ticker)
    print("=============recommendt_list_pass===============")
    print (Srecommend_list)
    #sc.parallelize(Srecommend_list).repartition(1).saveAsTextFile("file:/Users/nancywu/sparkhadoop/datatest_result/" + Target_Ticker+".rec")
    #writeToElastic("predictvalue",es,name,output)
    return Srecommend_list


class stock():
    def __init__(self,ticker, company,exchange, category, national, data):
        self.ticker=ticker,
        self.company=company,
        self.exchange=exchange,
        self.category=category,
        self.national=national,
        self.data=data


# 表单_recommendation_stock_chart
def search_recom(request):
    return render_to_response('search_recom.html')

def recommendation(request):
    #settings=[(u'spark.app.name', u'App'),(u'spark.cores.max',2)]
    #conf = SparkConf().setAll(settings)
    #sc = SparkContext(appName="app")
    request.encoding="utf-8"
    if "q" in request.GET:
        stock_ticker=request.GET["q"].encode("utf-8")
        Target_Ticker=stock_ticker
        similarity_length = 90
        #similarity_length: it is how long time the similarity you want to test
        recommend_number=6
        # recommend_number: how many stocks you want to recommend to your customers
        target=get_df_close_data(Target_Ticker)
        object_list = Similarity_Rec()
        print (object_list)
        #stock(stock_ticker,"","","","","")
        #return render(request, 'stock_details_chart.html',{'object_list':object_list})
        #return render(request, 'stock_details_chart.html',{'stock':stock})
        return HttpResponse (object_list)
    else:
        message = "Error"
        return HttpResponse(message)







