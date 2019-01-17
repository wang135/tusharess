# -*- coding: utf-8 -*-
"""
Created on Tue Dec 25 16:56:05 2018

@author: dell
"""

import tushare as ts
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy.types import VARCHAR
import datetime
import time
ww= ts
engine = create_engine('mysql+pymysql://root:1160329981wang@58edd9c77adb6.bj.cdb.myqcloud.com:5432/qianmancang',
                       echo=True)


def qianfuquan(code):
    a = ww.get_h_data(code)
    return a


def houfuquan(code):
    a = ww.get_h_data(code, autype='hfq')
    return a


def bufuquan(code):
    a = ww.get_h_data(code, autype=None)
    return a


if __name__ == '__main__':
    names = ww.get_today_all()['code'].values
    for code in names:
        qian = qianfuquan(code)
        qian.to_sql('qianfuquan', engine, if_exists='replace')
        time.sleep(10)
        hou = houfuquan(code)
        hou.to_sql('houfuquan', engine, if_exists='replace')
        time.sleep(10)
        bu = bufuquan(code)
        bu.to_sql('bufuquan', engine, if_exists='replace')
        time.sleep(200)
