# -*- coding: utf-8 -*-
"""
Created on Mon Dec 28 16:38:19 2020

@author: yeyedas
"""


import pandas as pd
from sqlalchemy import create_engine
import click
from utils import get_fund_data

@click.command()
@click.option('--account', default='root', help='Account of mysql')
@click.option('--password', default='123456', help='Password of mysql')
@click.option('--host', default='localhost', help='Host of mysql')
@click.option('--post', default='3306', help='Post of mysql')
@click.option('--database', default='fund', help='database of mysql')
@click.option('--update', default=True, help='Update data or create data')
@click.option('--save_error', default=False, help='Save error fund code')
def main_command(account, password, host, post, database, update, save_error):

    engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s'%(account,password,host,post,database))
    

    if update:
        
        fund_list = pd.read_sql('SELECT DISTINCT fund FROM fund_data_all', engine).sort_values(by='fund')
        date_last = pd.read_sql('SELECT DISTINCT date FROM fund_data_all WHERE fund=\'000001\'', engine).sort_values(by='date').iloc[-1,0]
    
    # fund_list = pd.read_sql_table('fund_list', engine)
    else:
        # 可以通过修改sql语句来控制下载哪些类型的基金
        # fund_list = pd.read_sql('SELECT DISTINCT fund FROM fund_list WHERE type=\'1\'', engine).sort_values(by='fund')
        fund_list = pd.read_sql('SELECT DISTINCT fund FROM fund_list', engine).sort_values(by='fund')
        date_last = None
    
    
    list_error = get_fund_data(fund_list, date_last, engine)
    
    print("\nfund_data_all更新完成")
    print("\n以下基金更新错误："+str(list_error))
    
    
    if save_error:
        
        file = open(r'~\fund_error.txt','w');
        file.write(str(list_error));    
        file.close();

    if update==False:
        with engine.connect() as con:
            con.execute('create index s1 on fund_data_all(fund(6))') # 创建索引
            con.execute('create index s2 on fund_data_all(date(12))') # 创建索引
            con.execute('create index s1 on fund_rate_all(fund(6))') # 创建索引
            con.close()
    
    return
            
if __name__ == '__main__':
    main_command()

    # 获取所有基金代码
    

