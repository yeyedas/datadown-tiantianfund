# -*- coding: utf-8 -*-
"""
Created on Mon Dec 28 16:38:19 2020

@author: yeyedas
"""

import requests
import pandas as pd
from sqlalchemy import create_engine
import time
import random
import re
import tqdm
from datetime import datetime
import click

def build_worth_data(fund_text, fund, date_last):

    pattern = '(?<=Data_netWorthTrend = \[).*(?=\];\/\*累计净值走势\*\/var)'
    unit_temp_index = re.search(pattern, fund_text).span()
    unit_temp = fund_text[unit_temp_index[0]:unit_temp_index[1]]
    unit_dict = eval(unit_temp)
    unit_df = pd.DataFrame(unit_dict)
    unit_df = unit_df[['x', 'y']].rename(columns={'x': 'date', 'y': 'netWorth'})
    unit_df.date = (unit_df.date * 1000000).astype('datetime64[ns, Asia/Shanghai]')
    unit_df.date = unit_df.date.dt.date.astype('str')
    unit_df['netWorth'] = unit_df['netWorth'].astype('float')
    unit_df['fund'] = fund

    pattern = '(?<=Data_ACWorthTrend = \[).*(?=\]\;\/\*累计收益率走势)'
    ACWorth_temp_index = re.search(pattern, fund_text).span()
    ACWorth_temp = fund_text[ACWorth_temp_index[0]:ACWorth_temp_index[1]]
    ACWorth_temp = ACWorth_temp.replace('null','None')
    ACWorth_list = eval(ACWorth_temp)
    ACWorth_df = pd.DataFrame(ACWorth_list, columns=['a', 'ACWorth'])
    ACWorth_df = ACWorth_df[['ACWorth']]

    unit_df = unit_df.join(ACWorth_df)
    unit_df = unit_df.loc[:, ['fund','date','netWorth','ACWorth']]
    unit_df = unit_df[unit_df.date >= date_last]
    if unit_df.ACWorth.isnull().any():
        index_list = unit_df[unit_df.ACWorth.isnull()].index
        for index_t in index_list:
            if index_t==0:
                unit_df.loc[index_t,'ACWorth'] = unit_df.loc[index_t,'netWorth']
            else:
                unit_df.loc[index_t, 'ACWorth'] = unit_df.loc[index_t - 1, 'ACWorth'] + (
                            unit_df.loc[index_t, 'netWorth'] - unit_df.loc[index_t - 1, 'netWorth'])
    unit_df['ACWorth']= unit_df['ACWorth'].astype('float')

    unit_df['growth'] = (unit_df['ACWorth'] - unit_df['ACWorth'].shift(1))/unit_df['netWorth'].shift(1)
    unit_df = unit_df[unit_df.date > date_last].reset_index(drop=True)
    if unit_df.loc[0,:].isnull().any():
        unit_df.loc[0,'growth'] = 0
    unit_df['growth'] = unit_df['growth'].astype('float')
    return unit_df

def get_fund_data(fund_list, engine, name, update):
    user_agent_list = [
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.4.3.4000 Chrome/30.0.1599.101 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE'
    ]

    # referer列表
    referer_list = [
        'http://fund.eastmoney.com/110022.html',
        'http://fund.eastmoney.com/110023.html',
        'http://fund.eastmoney.com/110024.html',
        'http://fund.eastmoney.com/110025.html'
    ]

    list_error = list()
    for fund in tqdm.tqdm(fund_list.fund):
        if update:
            date_last = pd.read_sql('SELECT MAX(date) FROM %s_fund_data WHERE fund=\'%s\''%(name,fund), engine).iloc[-1, 0]
        else:
            date_last = '2015-01-01'
        header = {'User-Agent': random.choice(user_agent_list),
                  'Referer': random.choice(referer_list)
                  }
        for i in range(5):
            try:
                fund_text = requests.get('http://fund.eastmoney.com/pingzhongdata/%s.js' % (fund), timeout=1,
                                         headers=header).text
                break
            except Exception:
                header = {'User-Agent': random.choice(user_agent_list),
                          'Referer': random.choice(referer_list)
                          }
                time.sleep(random.randint(15, 20) / 10)
        pattern = '(?<=测试数据 \* \@type \{arry\} \*//\*).*(?=\*/var ishb\=false)'
        fund_date_new_idx = re.search(pattern, fund_text).span()
        fund_date_new = pd.to_datetime(fund_text[fund_date_new_idx[0]:fund_date_new_idx[1]]).date()
        if (date_last < (fund_date_new - pd.Timedelta(days=1)).strftime("%Y-%m-%d")):
            try:
                worth_df = build_worth_data(fund_text, fund, date_last)
                if worth_df.empty == False:
                    if ((update == False) & (fund == fund_list.fund.iloc[0])):
                        worth_df.to_sql(name='%s_fund_data' % (name), con=engine, index=False, if_exists='replace')
                    else:
                        worth_df.to_sql(name='%s_fund_data' % (name), con=engine, index=False, if_exists='append')
            except Exception:
                list_error.append(fund)
                print('\n该基金下载错误：' + fund)
    return list_error


@click.command()
@click.option('--account', default='root', help='Account of mysql')
@click.option('--password', default='123456', help='Password of mysql')
@click.option('--host', default='localhost', help='Host of mysql')
@click.option('--post', default='3306', help='Post of mysql')
@click.option('--database', default='fund_data', help='database of mysql')
@click.option('--update', default='False', help='Update data or create data')

def main_command(account, password, host, post, database, update):
    # 获取所有基金数据
    if (update == 'True'):
        update = True
    if (update == 'False'):
        update = False
    engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s' % (account, password, host, post, database))
    names = ['hybrid', 'bond', 'equity', 'index', 'qdii', 'commodity']  # [混合，债券，股票，指数，QDII，商品（非QDII）]
    type_list = [1, 5, 9, 10, 11, 12, 13]

    for i in range(6):

        name = names[i]
        if update:
            fund_list = pd.read_sql('SELECT DISTINCT fund FROM %s_fund_data' % (name), engine).sort_values(by='fund')
        else:
            fund_list = pd.read_sql('SELECT DISTINCT fund FROM fund_list WHERE type BETWEEN %d AND %d' % (
            type_list[i], type_list[i + 1] - 1), engine).sort_values(by='fund')

        list_error = get_fund_data(fund_list, engine, name, update)

        print("\n%s_fund_data更新完成" % (names[i]))
        print("\n以下基金更新错误：" + str(list_error))
        f = open(r".\%s_fund_data_error_list.txt" % (name), "w")
        f.write(str(list_error))
        f.close()

        if update == False:
            with engine.connect() as con:
                con.execute('create index s1 on %s_fund_data(fund(6))' % (name))  # 创建索引
                con.execute('create index s2 on %s_fund_data(date(12))' % (name))  # 创建索引
                con.execute('create index s3 on %s_fund_data(fund(6),date(12))' % (name))  # 创建索引
                con.close()

    return

if __name__ == '__main__':
    main_command()

    

