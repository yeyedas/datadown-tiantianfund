# -*- coding: utf-8 -*-
"""
Created on Mon Dec 28 16:38:19 2021

@author: yeyedas
"""

'''
cal hybrid_fund_data to hybrid_earn_var
'''

import pandas as pd
from sqlalchemy import create_engine
import tqdm
import click

def cal_rank(date_list, name, update, engine):
    cal_earn_day = ['7days_earn', '30days_earn', '90days_earn', '180days_earn', '270days_earn', '365days_earn',
             'second_increase', 'third_increase', 'fourth_increase']
    cal_var_day = ['7days_var', '30days_var', '90days_var', '180days_var', '270days_var', '365days_var',
             'second_increase', 'third_increase', 'fourth_increase']

    for date in tqdm.tqdm(date_list.date):

        fund_data = pd.read_sql('SELECT * FROM %s_earn WHERE date=\'%s\'' % (name, date),engine)
        fund_data = fund_data.drop_duplicates()
        fund_earn_final = fund_data.loc[:,['fund','date']]
        for cal_item in cal_earn_day:
            fund_data_cal = fund_data.loc[:, ['fund', '%s' % (cal_item)]].dropna()
            fund_data_cal.loc[:, '%s_rank' % (cal_item)] = fund_data_cal.loc[:, '%s' % (cal_item)].rank(
                method='max')
            fund_data_cal.loc[:, '%s_rate' % (cal_item)] = fund_data_cal.loc[:, '%s_rank' % (cal_item)] / \
                                                           fund_data_cal.shape[0]
            fund_data_cal = fund_data_cal.drop(columns='%s' % (cal_item))
            fund_earn_final = fund_earn_final.merge(fund_data_cal,on='fund',how='left')

        fund_data = pd.read_sql('SELECT * FROM %s_var WHERE date=\'%s\'' % (name, date), engine)
        fund_data = fund_data.drop_duplicates()
        fund_var_final = fund_data.loc[:, ['fund', 'date']]
        for cal_item in cal_var_day:
            fund_data_cal = fund_data.loc[:, ['fund', '%s' % (cal_item)]].dropna()
            fund_data_cal.loc[:, '%s_rank' % (cal_item)] = fund_data_cal.loc[:, '%s' % (cal_item)].rank(
                ascending=False,method='max')
            fund_data_cal.loc[:, '%s_rate' % (cal_item)] = fund_data_cal.loc[:, '%s_rank' % (cal_item)] / \
                                                           fund_data_cal.shape[0]
            fund_data_cal = fund_data_cal.drop(columns='%s' % (cal_item))
            fund_var_final = fund_var_final.merge(fund_data_cal, on='fund', how='left')


        if fund_earn_final.empty == False:
            if (update == False) & (date == date_list.loc[0].values):
                fund_earn_final.to_sql(name='%s_earn_rank' % (name), con=engine, index=False, if_exists='replace')
            else:
                fund_earn_final.to_sql(name='%s_earn_rank' % (name), con=engine, index=False, if_exists='append')
        if fund_var_final.empty == False:
            if (update == False) & (date == date_list.loc[0].values):
                fund_var_final.to_sql(name='%s_var_rank' % (name), con=engine, index=False, if_exists='replace')
            else:
                fund_var_final.to_sql(name='%s_var_rank' % (name), con=engine, index=False, if_exists='append')

@click.command()
@click.option('--account', default='root', help='Account of mysql')
@click.option('--password', default='123456', help='Password of mysql')
@click.option('--host', default='localhost', help='Host of mysql')
@click.option('--post', default='3306', help='Post of mysql')
@click.option('--database', default='fund_data', help='database of mysql')
@click.option('--update', default='False', help='Update data or create data')

def main_command(account, password, host, post, database, update):

    if (update == 'True'):
        update = True
    if (update == 'False'):
        update = False
    engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s' % (account, password, host, post, database))
    names = ['hybrid', 'bond', 'equity', 'index', 'qdii', 'commodity']

    for name in names:
        # A = time.time()
        date_latest = pd.read_sql('SELECT MAX(date) FROM %s_earn' % (name), engine).loc[0, 'MAX(date)']

        # print(time.time()-A)


        if update:
            date_end = \
            pd.read_sql('SELECT MAX(date) FROM %s_earn_rank' % (name),
                        engine).iloc[0, 0]  # 当前数据最后日期
        else:
            date_end = '2015-01-01'
        if (date_latest>date_end):
            date_list = pd.read_sql('SELECT DISTINCT date FROM %s_earn WHERE date>\'%s\' ORDER BY date' % (name, date_end), engine)
            cal_rank(date_list, name, update, engine)

        if update == False:
            with engine.connect() as con:
                con.execute('create index s1 on %s_earn_rank(fund(6))' % (name))  # 创建索引
                con.execute('create index s2 on %s_earn_rank(date(12))' % (name))  # 创建索引
                con.execute('create index s3 on %s_earn_rank(fund(6),date(12))' % (name))  # 创建索引
                con.execute('create index s1 on %s_var_rank(fund(6))' % (name))  # 创建索引
                con.execute('create index s2 on %s_var_rank(date(12))' % (name))  # 创建索引
                con.execute('create index s3 on %s_var_rank(fund(6),date(12))' % (name))  # 创建索引
                con.close()


if __name__ == '__main__':
    main_command()