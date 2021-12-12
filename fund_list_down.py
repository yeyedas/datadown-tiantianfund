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
    fund_text = fund_text.replace('var r = ', '')
    fund_text = fund_text.replace(';', '')
    fund_text = fund_text.replace('混合型-偏股', '1')
    fund_text = fund_text.replace('混合型-灵活', '2')
    fund_text = fund_text.replace('混合型-偏债', '3')
    fund_text = fund_text.replace('混合型-平衡', '4')
    fund_text = fund_text.replace('债券型-中短债', '5')
    fund_text = fund_text.replace('债券型-混合债', '6')
    fund_text = fund_text.replace('债券型-长债', '7')
    fund_text = fund_text.replace('债券型-可转债', '8')
    fund_text = fund_text.replace('股票型', '9')
    fund_text = fund_text.replace('指数型-股票', '10')
    fund_text = fund_text.replace('商品（不含QDII）', '11')
    fund_text = fund_text.replace('QDII', '12')
    fund_text = fund_text.replace('货币型', '13')

    fund_js = eval(fund_text)
    fund_list = list()
    for i in range(len(fund_js)):
        fund_list.append([fund_js[i][0], fund_js[i][2], fund_js[i][3]])

    fund_df = pd.DataFrame(fund_list, columns=['fund', 'name', 'type'])
    fund_df['type'] = pd.to_numeric(fund_df['type'], errors='coerce')
    fund_df.dropna(inplace=True)
    fund_df['type'] = fund_df['type'].astype(int).astype(str)
    fund_df.drop(fund_df[fund_df.name.str.contains('后端')].index, axis=0, inplace=True)
    fund_df = fund_df.drop_duplicates()

    fund_df.to_sql(name='fund_list', con=engine, index=False, if_exists='replace')

    with engine.connect() as con:
        con.execute('create index type_s on fund_list(type(2))')  # 创建索引
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
    
