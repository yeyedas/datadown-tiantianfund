# -*- coding: utf-8 -*-
"""
Created on Mon Dec 28 16:38:19 2020

@author: yeyedas
"""

import requests
import pandas as pd
from sqlalchemy import create_engine

import click

def fund_list_down(engine):
    fund_text = requests.get('http://fund.eastmoney.com/js/fundcode_search.js').text
    fund_text = fund_text.replace('var r = ','')
    fund_text = fund_text.replace(';','')
    fund_text = fund_text.replace('混合型','1')
    fund_text = fund_text.replace('债券型','2')
    fund_text = fund_text.replace('股票型','3')
    fund_text = fund_text.replace('QDII-指数','4')
    fund_text = fund_text.replace('货币型','5')
    fund_text = fund_text.replace('定开债券','6')
    fund_text = fund_text.replace('联接基金','7')
    fund_text = fund_text.replace('股票指数','8')
    fund_text = fund_text.replace('QDII','9')
    fund_text = fund_text.replace('理财型','10')
    fund_text = fund_text.replace('债券指数','11')
    fund_text = fund_text.replace('保本型','12')
    fund_text = fund_text.replace('其他创新','13')
    fund_text = fund_text.replace('混合-FOF','14')
    fund_text = fund_text.replace('股票-FOF','15')
    fund_text = fund_text.replace('固定收益','16')
    fund_text = fund_text.replace('分级杠杆','17')
    fund_text = fund_text.replace('ETF-场内','18')
    fund_text = fund_text.replace('QDII-ETF','19')
    
    fund_js = eval(fund_text)
    fund_list = list()
    for i in range(len(fund_js)):
        fund_list.append([fund_js[i][0],fund_js[i][3]])
        
    fund_df = pd.DataFrame(fund_list, columns=['fund', 'type']) 
    
    
    
    fund_df.to_sql(name='fund_list', con=engine, index=False, if_exists='replace')
    
    with engine.connect() as con:
        con.execute('create index type_s on fund_list(type(2))') # 创建索引

        con.close()
    

    return

@click.command()
@click.option('--account', default='root', help='Account of mysql')
@click.option('--password', default='123456', help='Password of mysql')
@click.option('--host', default='localhost', help='Host of mysql')
@click.option('--post', default='3306', help='Post of mysql')
@click.option('--database', default='fund', help='database of mysql')

def main_command(account, password, host, post, database):
    engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s'%(account,password,host,post,database))
    fund_list_down(engine)
    print("fund_list更新完成")
    return
    
if __name__ == '__main__':
    main_command()
    
