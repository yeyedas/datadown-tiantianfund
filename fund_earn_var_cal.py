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

def cal_day_earn_and_var(fund_list, date_latest, date_end, name, update, engine):
    cal_day = [7, 30, 90, 180, 270, 365]

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

        earn_df = pd.DataFrame()
        earn_df.loc[:, 'date'] = fund_data.date
        earn_df.loc[:, 'fund'] = fund
        var_df = pd.DataFrame()
        var_df.loc[:, 'date'] = fund_data.date
        var_df.loc[:, 'fund'] = fund
        for day in cal_day:
            days_grand_total = (fund_data.ACWorth - fund_data.ACWorth.shift(day)) / fund_data.ACWorth.shift(day)
            earn_df.loc[:, '%ddays_earn' % (day)] = days_grand_total
        for day in cal_day:
            var_df.loc[:, '%ddays_var' % (day)] = fund_data.ACWorth.rolling(day + 1).var()

        earn_df.loc[:, 'second_increase'] = earn_df.loc[:, '90days_earn'].shift(90)
        earn_df.loc[:, 'third_increase'] = earn_df.loc[:, 'second_increase'].shift(90)
        earn_df.loc[:, 'fourth_increase'] = earn_df.loc[:, 'third_increase'].shift(90)

        var_df.loc[:, 'second_increase'] = var_df.loc[:, '90days_var'].shift(90)
        var_df.loc[:, 'third_increase'] = var_df.loc[:, 'second_increase'].shift(90)
        var_df.loc[:, 'fourth_increase'] = var_df.loc[:, 'third_increase'].shift(90)

        earn_df = earn_df.merge(date_list, how='right').dropna(subset=['7days_earn'])
        earn_df = earn_df[earn_df.date > date_end]
        var_df = var_df.merge(date_list, how='right').dropna(subset=['7days_var'])
        var_df = var_df[var_df.date > date_end]

        earn_df = earn_df[
            ['fund', 'date', '7days_earn', '30days_earn', '90days_earn', '180days_earn', '270days_earn', '365days_earn',
             'second_increase', 'third_increase', 'fourth_increase']]
        var_df = var_df[
            ['fund', 'date', '7days_var', '30days_var', '90days_var', '180days_var', '270days_var', '365days_var',
             'second_increase', 'third_increase', 'fourth_increase']]
        if earn_df.empty == False:
            if (update == False) & (fund == fund_list.loc[0].values):
                earn_df.to_sql(name='%s_earn' % (name), con=engine, index=False, if_exists='replace')
            else:
                earn_df.to_sql(name='%s_earn' % (name), con=engine, index=False, if_exists='append')
        if var_df.empty == False:
            if (update == False) & (fund == fund_list.loc[0].values):
                var_df.to_sql(name='%s_var' % (name), con=engine, index=False, if_exists='replace')
            else:
                var_df.to_sql(name='%s_var' % (name), con=engine, index=False, if_exists='append')

@click.command()
@click.option('--account', default='root', help='Account of mysql')
@click.option('--password', default='123456', help='Password of mysql')
@click.option('--host', default='localhost', help='Host of mysql')
@click.option('--post', default='3306', help='Post of mysql')
@click.option('--database', default='fund_data', help='database of mysql')
@click.option('--update', default=False, help='Update data or create data')

def main_command(account, password, host, post, database, update):

    engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s' % (account, password, host, post, database))
    names = ['hybrid', 'bond', 'equity', 'index', 'qdii', 'commodity']

    for name in names:
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
    main_command()