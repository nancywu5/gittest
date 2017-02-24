# -*- coding: utf-8 -*-
#from django.http import HttpResponse
from django.shortcuts import render,render_to_response
from django.db import models
from django.http import HttpResponseRedirect,HttpResponse
from elasticsearch import Elasticsearch

es =Elasticsearch (["10.1.2.3:9200"])
stockdict={}

class stock():
    def __init__(self,ticker, company,exchange, category, national, data):
        self.ticker=ticker,
        self.company=company,
        self.exchange=exchange,
        self.category=category,
        self.national=national,
        self.data=data

#从Elasticsearch 读取信息：
#{u'GOOG': [[u'Alphabet Inc.', u'NMS', u'USA', u'Internet Information Providers'], [5.0, 2.0, 2.5, 4.0, 3.0]]}
def Es_reader(stocksearch):
    res = es.search(index=stocksearch)
    for i in res['hits']['hits']:
        x=i['_source']
        stockdict[x['ticker']]=[x['details'],x['pentagon']]

# 表单_pentagon
def search_stock(request):
    return render_to_response('search_stock.html')



# 接收请求数据_pentagon
def search(request):
    request.encoding="utf-8"
    if "q" in request.GET:
        stock_ticker=request.GET["q"].encode("utf-8")
        #此处stocksearch为es的index;"stocklongtermd", "stockshortermd"
        Es_reader("stockshortermd")
        details=stockdict.get(stock_ticker)[0]
        pentagons=stockdict.get(stock_ticker)[1]
        object_list = [stock(stock_ticker,details[0],details[1],details[2],details[3],pentagons)]
        #object_list=Es_data(stock_ticker)
        return render(request, 'radar_individual.html',{'object_list':object_list})
    else:
        message = "Error"
        return HttpResponse(message)


# 表单_detail stock_chart
def search_stock_detail(request):
    return render_to_response('search_stock_detail.html')

def detail(request):
    request.encoding="utf-8"
    if "q" in request.GET:
        stock_ticker=request.GET["q"].encode("utf-8")
        object_list = [stock(stock_ticker,"","","","","")]
        #stock(stock_ticker,"","","","","")
        return render(request, 'stock_details_chart.html',{'object_list':object_list})
        #return render(request, 'stock_details_chart.html',{'stock':stock})
    else:
        message = "Error"
        return HttpResponse(message)
