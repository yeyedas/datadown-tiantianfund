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

def cal_day_earn_and_var(fund_list, date_latest, date_end, name, update, engine):
    cal_day_earn = [30, 90, 180, 270, 365]
    cal_day_var = [90, 365]

    for fund in tqdm.tqdm(fund_list.fund):

        fund_data = pd.read_sql('SELECT fund,date,ACWorth FROM %s_fund_data WHERE fund=\'%s\'' % (name, fund),
                                engine).sort_values(by='date')
        fund_data = fund_data.drop_duplicates()
        fund_data = fund_data[
            fund_data.date >= (pd.to_datetime(date_end) - pd.Timedelta(days=365) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")]
        date_list = fund_data.date

        date_list_full = pd.DataFrame()
        date_list_full.loc[:, 'date'] = pd.date_range(date_list.iloc[0], date_latest)
        date_list_full.loc[:, 'date'] = date_list_full.loc[:, 'date'].astype('str')

        fund_data = fund_data.merge(date_list_full, how="outer", on="date").sort_values(by='date').reset_index(
            drop=True)
        fund_data = fund_data.fillna(method='ffill')

        df = pd.DataFrame()
        df.loc[:, 'date'] = fund_data.date
        df.loc[:, 'fund'] = fund
        for day in cal_day_earn:
            days_grand_total = (fund_data.ACWorth - fund_data.ACWorth.shift(day)) / fund_data.ACWorth.shift(day)
            df.loc[:, '%ddays_earn' % (day)] = days_grand_total
        for day in cal_day_var:
            df.loc[:, '%ddays_var' % (day)] = fund_data.ACWorth.rolling(day + 1).var()

        df.loc[:, 'second_increase'] = df.loc[:, '90days_earn'].shift(90)
        df.loc[:, 'third_increase'] = df.loc[:, 'second_increase'].shift(90)
        df.loc[:, 'fourth_increase'] = df.loc[:, 'third_increase'].shift(90)

        df = df.merge(date_list, how='right').dropna()
        df = df[df.date > date_end]

        df = df[['fund', 'date', '30days_earn', '90days_earn', '180days_earn', '270days_earn', '365days_earn',
                 'second_increase', 'third_increase', 'fourth_increase', '90days_var', '365days_var']]
        if df.empty == False:
            if (update == False) & (fund == fund_list.loc[0].values):
                df.to_sql(name='%s_earn_var' % (name), con=engine, index=False, if_exists='replace')
            else:
                df.to_sql(name='%s_earn_var' % (name), con=engine, index=False, if_exists='append')


def main_command(engine, update):

    name = 'hybrid'

    date_latest = pd.read_sql('SELECT MAX(date) FROM %s_fund_data' % (name), engine).loc[0, 'MAX(date)']
    fund_list = pd.read_sql('SELECT DISTINCT fund FROM %s_fund_data WHERE date=\'%s\'' % (name, date_latest), engine)\
        .sort_values(by='fund')

    if update:
        date_end = \
        pd.read_sql('SELECT MAX(date) FROM %s_earn_var WHERE fund=\'%s\'' % (name, fund_list.iloc[0, 0]),
                    engine).iloc[0, 0]  # 当前数据最后日期
    else:
        date_end = '2015-01-01'
    cal_day_earn_and_var(fund_list, date_latest, date_end, name, update, engine)

    if update == False:
        with engine.connect() as con:
            con.execute('create index s1 on %s_earn_var(fund(6))' % (name))  # 创建索引
            con.execute('create index s2 on %s_earn_var(date(12))' % (name))  # 创建索引
            con.execute('create index s3 on %s_earn_var(fund(6),date(12))' % (name))  # 创建索引
            con.close()


if __name__ == '__main__':
    account = 'root'
    password = 'Aa123321123'
    host = 'localhost'
    post = '3306'
    database = 'fund'
    update = False
    engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s' % (account, password, host, post, database))
    # date_latest = pd.read_sql('SELECT MAX(date) FROM equity_fund_data WHERE fund=\'000082\'', engine).astype('datetime64').iloc[0,0] # 基础数据最后日期
    main_command(engine, update)