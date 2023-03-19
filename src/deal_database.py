import os,sys,re
import sqlite3
import threading

import xlwt

#sql 生成数据库
class SQLiteWraper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """
    def __init__(self,path,command='',*args,**kwargs):
        self.lock = threading.RLock() #锁
        self.path = path #数据库连接参数

        if command!='':
            conn=self.get_conn()
            cu=conn.cursor()
            cu.execute(command)

    def get_conn(self):
        conn = sqlite3.connect(self.path)#,check_same_thread=False)
        conn.text_factory=str
        return conn

    def conn_close(self,conn=None):
        conn.close()

    def conn_trans(func):
        def connection(self,*args,**kwargs):
            self.lock.acquire()
            conn = self.get_conn()
            kwargs['conn'] = conn
            rs = func(self,*args,**kwargs)
            self.conn_close(conn)
            self.lock.release()
            return rs
        return connection

    @conn_trans
    def execute(self,command,method_flag=0,conn=None):
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0],command[1])
            conn.commit()
        except sqlite3.IntegrityError as fe:
            #print( e)
            return -1
        except Exception as  e:
            print( e)
            return -2
        return 0

    @conn_trans
    def fetchall(self,command="select name, subway from xiaoqu",conn=None):
        cu=conn.cursor()
        lists=[]
        try:
            cu.execute(command)
            lists=cu.fetchall()
        except Exception as e:
            print( e)
            pass
        return lists
def gen_xiaoqu_insert_command(info_dict):
    """
    生成小区数据库插入命令
    """
    info_list=[u'小区名称',u'大区域',u'小区域',u'小区户型',u'建造时间',u'地铁',u'均价',u'成交网址',u'在售网址']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into xiaoqu values(?,?,?,?,?,?,?,?,?)",t)
    return command
def gen_chengjiao_insert_command(info_dict):
    """
    生成成交记录数据库插入命令
    """
    info_list=[u'链接',u'小区名称',u'户型',u'面积',u'朝向',u'楼层',u'建造时间',u'签约时间',u'签约单价',u'签约总价',u'房产类型',u'学区',u'地铁']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into chengjiao values(?,?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command

def xls_cellwidth_adj( str_cell ):
    'func： 根据cell字符串长度，返回cell合适长度'
    'ret: ui_cellwidth'
    ui_code_fmt = 2 #中文编码*2, 英文*1
    if( len(re.findall( 'html', str_cell )) ):
        ui_code_fmt = 1
    ui_cellwidth = 2356 #2356 = std_cellwidth
    ui_str_len = len(str_cell) * ui_code_fmt #中文编码*2
    ui_str_len = (ui_str_len+7)//8 #标准cell款存放8个char
    ui_cellwidth = ui_cellwidth * ui_str_len

    return ui_cellwidth

#过滤xq_db: 均价7W以下,建成年代2000年以后;
def filter_xq_db( fi_db, fo_db, dict_json={"建成时间":2000,"单价":80000} ):
    'func: 过滤xq_db: 均价7W以下,建成年代2000年以后'
    conn = sqlite3.connect(fi_db)
    cu = conn.cursor()

    ui_build_year = dict_json["建成时间"]
    ui_unit_price = dict_json["单价"]
    print( 'NOTICE(), 把数据库:"{}" 过滤掉均价{}以上，建成年代{}年以前的小区, 过滤后写到:"{}"'.format( fi_db, ui_unit_price,ui_build_year, fo_db ) )
    #获取所有db内容
    list_row=[]
    command="SELECT * from xiaoqu"
    cu.execute(command)
    list_row=cu.fetchall()
    cu.close()

    #创建过滤后的db
    command="CREATE table if not exists xiaoqu (name TEXT primary key UNIQUE, regionb TEXT, regions TEXT, style TEXT, year TEXT, subway TEXT, price TEXT, url_cj TEXT, url_zs TEXT)"
    db_flt_xq=SQLiteWraper(fo_db,command)

    dict_xq_idx2val={0:'小区名称',1:u'大区域',2:u'小区域',3:u'小区户型',4:u'建造时间',5:u'地铁',6:u'均价',7:u'成交网址',8:u'在售网址'}
    dict_xq_val2idx = {'建造时间':4, '均价':6}
    for i in range(len(list_row)):
        str_year = list_row[i][ dict_xq_val2idx['建造时间'] ]
        str_price= list_row[i][ dict_xq_val2idx['均价'] ]
        li_str_price = str_price.split(',')
        if( len(li_str_price)>=2 ):
            str_price = li_str_price[0]+li_str_price[1]
        lst_year  = re.findall( '\d+', str_year  )
        lst_price = re.findall( '\d+', str_price )
        ui_year  = 0
        if( len(lst_year ) ):
            ui_year = int(lst_year [0])
        ui_price = 1000000
        if( len(lst_price) ):
            ui_price= int(lst_price[0])
        info_dict = {}
        if( ui_year>=ui_build_year and ui_price<ui_unit_price ):
            col_cnt = 0
            one_row = list_row[i]
            for val in one_row:
                info_dict[ dict_xq_idx2val[col_cnt] ] = val
                col_cnt += 1
            command=gen_xiaoqu_insert_command(info_dict)
            db_flt_xq.execute(command,1)

#把数据库转换为excel
def deal_db2xls(fn_db, fn_xls):
    'param, fn_db：数据库文件名, fn_xls：excel文件名'
    "xq_info_list=[u'小区名称',u'大区域',u'小区域',u'小区户型',u'建造时间',u'地铁',u'均价']"
    "cj_info_list=[u'链接',u'小区名称',u'户型',u'面积',u'朝向',u'楼层',u'建造时间',u'签约时间',u'签约单价',u'签约总价',u'房产类型',u'学区',u'地铁']"
    conn = sqlite3.connect(fn_db)
    cu = conn.cursor()

    print( 'NOTICE(), 把数据库:"{}" 转换成表格:"{}"'.format( fn_db, fn_xls ) )
    #获取db列名称
    list_name = []
    command="SELECT * FROM sqlite_master WHERE type='table'"
    cu.execute(command)
    list_name=cu.fetchone(); #print( list_name )
    # CREATE TABLE sqlite_master (type TEXT, name TEXT, (2)tbl_name TEXT, rootpage INTEGER, (4 or -1)sql TEXT );
    tbl_name = list_name[2]
    list_sql  = re.findall( "\((.*)\)", list_name[-1] )[0].split(','); #print( 'table_name:',tbl_name, '\nsql:', list_sql )

    #创建excel第一行
    str_sheetname = fn_xls.split('.')[0]
    ui_cellwidth = 1178 #2356 = std_cellwidth
    workbook = xlwt.Workbook(encoding='unicode')
    worksheet = workbook.add_sheet(str_sheetname)
    alighment = xlwt.Alignment()
    alighment.horz = 2  # 0:通用， 1-3:左中右
    alighment.vert = 1  # 0-2：上中下
    alighment.wrap = 1  # 1：自动换行
    style = xlwt.XFStyle()
    style.alignment = alighment

    for i in range(len(list_sql)):
        col_name = list_sql[i].split()[0]
        worksheet.write(0, i, col_name , style)
        # worksheet.col(i).width = ui_cellwidth * 4

    #获取所有db内容
    list_row=[]
    command="SELECT * from {}".format( tbl_name )
    cu.execute(command)
    list_row=cu.fetchall()

    #把db内容写到excel中
    row_cnt = 1
    col_cnt = 1
    dict_xq = {'建造时间':4, '均价':6}
    for i in range(len(list_row)):
        # str_year = list_row[i][ dict_xq['建造时间'] ]
        # str_price= list_row[i][ dict_xq['均价'] ]
        # a = re.findall( '\d+', str_year )
        # b = re.findall( '\d+', str_price )
        one_row = list_row[i]
        col_cnt = 0
        for val in one_row:
            worksheet.write(row_cnt, col_cnt, val, style)
            col_cnt += 1
        row_cnt += 1
    #end of for list_row

    #调整表格单元宽度， 在前20行中找每列最大字符， 填充到list_one_row
    list_one_row = list(list_row[0])
    for i in range(20):
        one_row = list_row[ min(i,len(list_row)-1) ]
        for col_idx in range(len(one_row)):
            if( len(one_row[col_idx]) > len(list_one_row[col_idx]) ):
                list_one_row[col_idx] = one_row[col_idx]
    col_cnt = 0
    for str_val in list_one_row:
        cellwidth = xls_cellwidth_adj( str_val ); #print( str_val, len(str_val), cellwidth )
        worksheet.col(col_cnt).width = cellwidth
        col_cnt += 1

    workbook.save(fn_xls)

if __name__=="__main__":
    fn_db     = 'lianjia-xq.db'
    fn_db_flt = 'lianjia-xq_filter.db'
    fn_db_cj  = 'lianjia-cj.db'
    filter_xq_db( fn_db, fn_db_flt )

    fn_xls= 'lianjia-xq.xls'
    deal_db2xls( fn_db, fn_xls )
    fn_xls= 'lianjia-xq_filter.xls'
    deal_db2xls( fn_db_flt, fn_xls )
    fn_xls= 'lianjia-cj.xls'
    deal_db2xls( fn_db_cj, fn_xls )