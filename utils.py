# -*- coding: utf-8 -*-
"""
Created on Fri Jan 15 15:54:26 2021

@author: Administrator
"""

import requests
import pandas as pd
import random
import re
import time
import tqdm
import datetime

def build_worth_data(fund_text, fund, date_last):
    if date_last!=None:
        date_end = int(datetime.datetime.strptime(date_last, "%Y-%m-%d").timestamp()*1000)
        
    pattern = '(?<=Data_netWorthTrend = \[).*(?=\];\/\*累计净值走势\*\/var)'
    unit_temp_index = re.search(pattern, fund_text).span()
    unit_temp = fund_text[unit_temp_index[0]:unit_temp_index[1]]
    unit_dict = eval(unit_temp)
    unit_list_change = list()
    for i in unit_dict:
        if date_last==None:
            unit_list_change.append([i["x"],i["y"],i["equityReturn"]])
        else:
            if i["x"] > date_end:
                unit_list_change.append([i["x"],i["y"],i["equityReturn"]])    
    unit_df = pd.DataFrame(unit_list_change, columns=['date', 'netWorth', 'growth']) 
    unit_df['date']= unit_df['date'].apply(lambda x:pd.Timestamp(int(x),unit='ms',tz='Asia/Shanghai').strftime("%Y-%m-%d"))
    unit_df['netWorth']= unit_df['netWorth'].astype('float')
    unit_df['growth']= unit_df['growth'].astype('float')
    
    unit_df['fund']= fund
    
    pattern = '(?<=Data_ACWorthTrend = \[).*(?=\]\;\/\*累计收益率走势)'
    ACWorth_temp_index = re.search(pattern, fund_text).span()

    ACWorth_temp = fund_text[ACWorth_temp_index[0]:ACWorth_temp_index[1]]
    ACWorth_temp = ACWorth_temp.replace("null", "0")
    ACWorth_list = eval(ACWorth_temp)
    ACWorth_list_change = list()

    for i in ACWorth_list:
        if date_last==None:
            ACWorth_list_change.append([i[1]])
        else:
            if i[0] > date_end:
                ACWorth_list_change.append([i[0],i[1]])
    ACWorth_df = pd.DataFrame(ACWorth_list_change, columns=['date','ACWorth'])
    ACWorth_df['date']= ACWorth_df['date'].apply(lambda x:pd.Timestamp(int(x),unit='ms',tz='Asia/Shanghai').strftime("%Y-%m-%d"))
    ACWorth_df['ACWorth']= ACWorth_df['ACWorth'].astype('float')
    
    unit_df = pd.merge(unit_df, ACWorth_df)
    
    error_t = unit_df[unit_df['ACWorth']==0].index
    
    for i in range(len(error_t)):
        unit_df.loc[error_t[i],'ACWorth'] = unit_df.loc[error_t[i],'netWorth']
        unit_df.loc[error_t[i],'growth'] = (unit_df.loc[error_t[i],'ACWorth']-unit_df.loc[error_t[i-1],'ACWorth'])/unit_df.loc[error_t[i-1],'netWorth']
        
    unit_df = unit_df.loc[:, ['fund','date','netWorth','ACWorth','growth']]

    return unit_df

def build_rate_data(fund_text, fund, date_last):
    
    if date_last!=None:
        date_end = int(datetime.datetime.strptime(date_last, "%Y-%m-%d").timestamp()*1000)
        
    pattern = '(?<=Data_rateInSimilarType = \[).*(?=\]\;\/\*同类排名百分比)'
    rateType_temp_index = re.search(pattern, fund_text).span()
    rateType_temp = fund_text[rateType_temp_index[0]:rateType_temp_index[1]]
    rateType_list = eval(rateType_temp)
    rateType_list_change = list()
    for i in rateType_list:
        if date_last==None:
            rateType_list_change.append([i["x"],i["y"]])
        else:
            if i["x"] > date_end:
                rateType_list_change.append([i["x"],i["y"]])
    rateType_df = pd.DataFrame(rateType_list_change, columns=['date','rateType'])
    rateType_df['date']= rateType_df['date'].apply(lambda x:pd.Timestamp(int(x),unit='ms',tz='Asia/Shanghai').strftime("%Y-%m-%d"))
    rateType_df['rateType']= rateType_df['rateType'].astype('int')
    
    pattern = '(?<=rateInSimilarPersent=\[).*(?=\]\;\/\*规模变动)'
    ratePersent_temp_index = re.search(pattern, fund_text).span()
    ratePersent_temp = fund_text[ratePersent_temp_index[0]:ratePersent_temp_index[1]]
    ratePersent_list = eval(ratePersent_temp)

    ratePersent_list_change = list()
    for i in ratePersent_list:
        if date_last==None:
            ratePersent_list_change.append([i[1]])
        else:
            if i[0] > date_end:
                ratePersent_list_change.append([i[1]])
        
    ratePersent_df = pd.DataFrame(ratePersent_list_change, columns=['ratePersent'])
    ratePersent_df['ratePersent']= ratePersent_df['ratePersent'].astype('float')
    
    rateType_df['fund'] = fund
    
    rateType_df = rateType_df.join(ratePersent_df)
    rateType_df = rateType_df.loc[:, ['fund','date','rateType','ratePersent']]

    return rateType_df



def get_fund_data(fund_list, date_last, engine):

    user_agent_list = [
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.4.3.4000 Chrome/30.0.1599.101 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36 QIHU 360SE'
    ]
    
    referer_list = [
        'http://fund.eastmoney.com/110022.html',
        'http://fund.eastmoney.com/110023.html',
        'http://fund.eastmoney.com/110024.html',
        'http://fund.eastmoney.com/110025.html'
    ]
    

    list_error = list()
    for fund in tqdm.tqdm(fund_list.fund):
        header = {'User-Agent': random.choice(user_agent_list),
                  'Referer': random.choice(referer_list)
                  }
        

        for i in range(5):
            try:
                fund_text = requests.get('http://fund.eastmoney.com/pingzhongdata/%s.js'%(fund), timeout=1, headers=header).text
                break
            except Exception:
                header = {'User-Agent': random.choice(user_agent_list),
                          'Referer': random.choice(referer_list)
                          }
                time.sleep(random.randint(15,20)/10)

        try:
            worth_df = build_worth_data(fund_text, fund, date_last)
            if worth_df.empty==False:
                worth_df.to_sql(name='fund_data_all', con=engine, index=False, if_exists='append')
        except Exception:
            list_error.append(fund)
            print('\n该基金下载错误：'+fund)

        try:
            rate_df = build_rate_data(fund_text, fund, date_last)
            if rate_df.empty==False:
                rate_df.to_sql(name='fund_rate_all', con=engine, index=False, if_exists='append')
        except Exception:
            continue
        
    return list_error
