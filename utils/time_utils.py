# -*- coding: utf-8 -*-
__docformat__ = 'NumPy'
import sys, os
import time
import datetime
from datetime import date, timedelta

def make_datelist(date_begin, date_num):
    # make a date_list YYYY-MM-DD
    d1 = [int(x) for x in date_begin.split('-')]
    mydt = date(year = d1[0], month = d1[1], day = d1[2])
    delta = timedelta(days = 1)
    count, date_list = 0, []
    while count < date_num:
        date_list.append(mydt.__str__())
        mydt = mydt + delta
        count += 1
    return date_list

def make_time_index(date_begin, date_num):
    date_list = make_datelist(date_begin, date_num)
    return [datetime.datetime.strptime(x, "%Y-%m-%d") for x in date_list]

