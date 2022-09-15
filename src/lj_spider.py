import os,sys,re
import threadpool
from bs4 import BeautifulSoup

import urllib
import random
import requests
import threading
import time,datetime
import numpy

from deal_database import *

# 北京行政区
# regions=[u"东城",u"西城",u"朝阳",u"海淀",u"丰台",u"石景山","通州",u"昌平",u"大兴",u"亦庄开发区",u"顺义",u"房山",u"门头沟",u"平谷",u"怀柔",u"密云",u"延庆",u"燕郊"]
# regions=[u"大兴",u"丰台"]
# regions=[u"朝阳",u"昌平",u"丰台"]

# 上海行政区
# regions=["浦东","闵行","宝山","徐汇","普陀","杨浦","长宁","松江","嘉定","黄浦","静安","虹口","青浦","奉贤","金山","崇明"]
# regions=["浦东","闵行","黄浦","徐汇","杨浦","普陀"]
regions=["浦东","徐汇","静安"]

#多线程
lock = threading.Lock()
RANDOM_DELAY = 1
def random_delay():
    if RANDOM_DELAY:
        time.sleep(random.randint(1, 10))

#http header， 防反爬虫
hds=[{'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
    {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
    {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
    {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]

#爬取小区
def do_xiaoqu_spider(db_xq,region=u"昌平"):
    """
    爬取大区域中的所有小区信息
    """
    # url=u"http://bj.lianjia.com/xiaoqu/rs"+region+"/"
    url=u"http://sh.lianjia.com/xiaoqu/rs"+region+"/"
    print( url )

    response = requests.get(url, timeout=10, headers=hds[random.randint(0,len(hds)-1)])
    html = response.content
    soup = BeautifulSoup(html, "lxml")

    d=soup.find('div',{'class':'page-box house-lst-page-box'}).get('page-data'); #print(d)
    dict_d=eval(d); #print(d) #转换为字典
    ui_total_pages=dict_d['totalPage']

    # 单线程
    # for i in range(ui_total_pages):
    #     url_page=u"http://bj.lianjia.com/xiaoqu/pg%drs%s/" % (i+1,region)
    #     xiaoqu_spider(db_xq, url_page)

    # 多线程
    threads=[]
    for i in range(ui_total_pages):
        url_page=u"http://sh.lianjia.com/xiaoqu/pg%drs%s/" % (i+1,region)
        t=threading.Thread(target=xiaoqu_spider,args=(db_xq,url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print( u"爬下了 %s 区全部的小区信息" % region)

def xiaoqu_spider(db_xq,url_page=u"http://sh.lianjia.com/xiaoqu/pg1rs%E6%98%8C%E5%B9%B3/"):
    """
    爬取页面链接中的小区信息
    """
    random_delay()
    response = requests.get(url_page, timeout=10, headers=hds[random.randint(0,len(hds)-1)])
    html = response.content
    soup = BeautifulSoup(html, "lxml")

    xiaoqu_list=soup.findAll('li',{'class':'clear xiaoquListItem'})
    for xq_li in xiaoqu_list:
        xq = xq_li.find('div',{'class':'info'})
        info_dict={}
        info_dict.update({u'小区名称':xq.find('a').text})

        content = xq.find('div',{'class':'positionInfo'}).text.strip(); #print( content )
        info = content.split(); #print( info )
        if( info ):
            info_dict.update({u'大区域':info[0]})
            info_dict.update({u'小区域':info[1]})
            info_dict.update({u'小区户型':info[2]})
            info_dict.update({u'建造时间':info[ numpy.clip(4,0,len(info)-1) ]})

        subway = xq.find('div',{'class':'tagList'}).text.strip()
        info_dict.update({u'地铁':subway})

        price = xq_li.find('div',{'class':'totalPrice'}); #print( price.text )
        info_dict.update({u'均价':price.text})

        # print( info_dict )
        command=gen_xiaoqu_insert_command(info_dict)
        db_xq.execute(command,1)


#爬取成交
def do_xiaoqu_chengjiao_spider(db_xq,db_cj):
    """
    批量爬取小区成交记录
    """
    count=0
    ui_housenum_xq = 0
    xq_list=db_xq.fetchall()
    for xq in xq_list:
        ui_housenum_xq = xiaoqu_chengjiao_spider(db_cj,xq[0],xq_subway=xq[1])
        count+=1
        print( 'have spidered {cnt} xiaoqu, num:{n} time:{t}'.format(cnt=count, n=ui_housenum_xq, t=datetime.datetime.now()) )
    print( 'done')
def xiaoqu_chengjiao_spider(db_cj,xq_name=u"冠庭园",xq_subway="10号线"):
    """
    爬取小区成交记录
    """
    url=u"http://sh.lianjia.com/chengjiao/rs"+urllib.request.quote(xq_name)+"/"
    response = requests.get(url, timeout=10, headers=hds[random.randint(0,len(hds)-1)])
    html = response.content
    soup = BeautifulSoup(html, "lxml")
    print( url )

    ui_total_pages = 0
    d_tmp=soup.find('div',{'class':'page-box house-lst-page-box'})
    if( d_tmp ):
        d=d_tmp.get('page-data'); #print(d)
        dict_d=eval(d); #print(d) #转换为字典
        ui_total_pages=dict_d['totalPage']

    #启动单线程爬取小区成交
    # for i in range(ui_total_pages):
    #     url_page=u"http://bj.lianjia.com/chengjiao/pg%drs%s/" % (i+1,urllib.request.quote(xq_name))
    #     chengjiao_spider( db_cj, xq_subway, url_page )

    # 启动多线程爬取小区成交
    threads=[]
    for i in range(ui_total_pages):
        url_page=u"http://sh.lianjia.com/chengjiao/pg%drs%s/" % (i+1,urllib.request.quote(xq_name))
        t=threading.Thread(target=chengjiao_spider,args=(db_cj,xq_subway,url_page))
        threads.append(t)
    for t in threads:
        pool_sema.acquire()
        t.start()
    for t in threads:
        t.join()
    time.sleep(8)
    return ui_total_pages
def chengjiao_spider(db_cj, xq_subway="10号线", url_page=u"http://sh.lianjia.com/chengjiao/pg1rs%E5%86%A0%E5%BA%AD%E5%9B%AD"):
    """
    爬取页面链接中的成交记录
    """
    random_delay()
    try:
        response = requests.get(url_page, timeout=10, headers=hds[random.randint(0,len(hds)-1)])
        html = response.content
        soup = BeautifulSoup(html, "lxml")
    except (urllib.request.HTTPError) as  e:
        print( 'HTTPError' )
    # print( url_page )

    cj_list=soup.findAll('div',{'class':'info'})

    ui_cj_notmeet_cnt = 0
    for cj in cj_list:
        info_dict={}
        ui_cj_notmeet_cnt = 0
        href=cj.find('a')
        if not href:
            continue
        info_dict.update({u'链接':href.attrs['href']})
        content=href.text.split(); #print( content )
        if( len(content) ):
            info_dict.update({u'小区名称':content[0]})
            info_dict.update({u'户型':content[ numpy.clip(1,0,len(content)-1) ]})
            info_dict.update({u'面积':content[ numpy.clip(2,0,len(content)-1) ]})

        if( ui_cj_notmeet_cnt>8 ):
            break
        content=cj.find('div',{'class':'dealDate'})
        if( content ):
            if( int(content.text.split('.')[0])<2018 ):
                ui_cj_notmeet_cnt +=1
                continue
        info_dict.update({u'签约时间':content.text})
        content=cj.find('div',{'class':'unitPrice'})
        info_dict.update({u'签约单价':content.text})
        content=cj.find('div',{'class':'totalPrice'})
        info_dict.update({u'签约总价':content.text})
        content=cj.find('div',{'class':'houseInfo'}).text; #print( content )
        info_dict.update({u'朝向':content.strip()})
        content=cj.find('div',{'class':'positionInfo'}).text; #print( content )
        if( content.split() ):
            info_dict.update({u'楼层':content.split()[0]})
            info_dict.update({u'建造时间':content.split()[1]})

        c = ''
        info_dict.update({u'房产类型':c})
        info_dict.update({u'学区':c})
        info_dict.update({u'地铁':xq_subway})
        content=cj.find('div',{'class':'dealHouseInfo'})
        if( content ):
            info_dict.update({u'房产类型':content.text})
        # print( info_dict )

        command=gen_chengjiao_insert_command(info_dict)
        db_cj.execute(command,1)
    #end of for cj_list
    pool_sema.release()


if __name__=="__main__":
    fn_xq     = 'lianjia-xq.db'
    fn_xq_flt = 'lianjia-xq_flt.db'
    fn_cj     = 'lianjia-cj.db'
    command="create table if not exists xiaoqu (name TEXT primary key UNIQUE, regionb TEXT, regions TEXT, style TEXT, year TEXT, subway TEXT, price TEXT)"
    db_xq=SQLiteWraper(fn_xq,command)
    command="create table if not exists chengjiao (href TEXT primary key UNIQUE, name TEXT, style TEXT, area TEXT, orientation TEXT, floor TEXT, year TEXT, trade_time TEXT, unit_price TEXT, total_price TEXT,fangchan_class TEXT, school TEXT, subway TEXT)"
    db_cj=SQLiteWraper(fn_cj,command)

    maxconnections = 8
    pool_sema = threading.BoundedSemaphore(value=maxconnections)
    #1.爬下所有的小区信息
    for region in regions:
        do_xiaoqu_spider(db_xq,region)

    #2.过滤买不起的小区，得到过滤后的db
    filter_xq_db( fn_xq, fn_xq_flt )
    db_xq_flt = SQLiteWraper(fn_xq_flt)
    db_xq_flt.get_conn()

    #3.爬下所有小区里的成交信息
    do_xiaoqu_chengjiao_spider(db_xq_flt,db_cj)

    #4.db转excel
    deal_db2xls( fn_xq_flt, fn_xq_flt.split('.')[0]+'.xls' )
    deal_db2xls( fn_cj, fn_cj.split('.')[0]+'.xls' )
