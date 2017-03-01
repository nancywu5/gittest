"""HelloWorld URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/dev/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import include, url
from django.conf import settings
from HelloWorld.view import hello
from django.conf.urls import *
from HelloWorld import search,recom

urlpatterns = [
		url(r'^hello/$', hello,name='helloworld'),
        url(r'^search/$', search.search_stock),
        url(r'^search-stock/$', search.search),
        url(r'^recom/$', recom.search_recom),
        url(r'^detail/$', recom.recommendation),
               #url(r'^search/detail/$', search.detail)
]
