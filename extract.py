# -*- coding: utf-8 -*-
__docformat__ = 'NumPy'

import numpy as np
import pandas as pd

"""
select concat(sessionid,"_",visid_high,"_",visid_low) as my_id,search_id, date_time,prop60
from taskforce_sv_search
where dt = "2016-04-01" and not isnull(prop60)
limit 100
"""

from collections import defaultdict
from utils import extract_utils, time_utils

def retrieve_simple_samples(df):
    """
    Remove rows where prop60 is different for given search_id
    """
    assert df.shape[1] == 3, print("column number not matched. Required 3 but saw %d"%df.shape[1])

    res_dict = defaultdict(list)
    for my_id, search_id, prop60 in df.values:
        mykey = (my_id, search_id)
        if prop60 not in res_dict[mykey]:
            res_dict[mykey].append(prop60)

    tokeep_list = []
    for key, val in res_dict.items():
        if len(val) == 1:
            tokeep_list.append((key[0], key[1], val[0]))

    return pd.DataFrame(tokeep_list, columns=['my_id', 'search_id', 'prop60'])

def extract_session_byDay(dt):
    out_filename = "session_reduced_%s"%dt
    print("retrieving session %s"%dt)
    with open("sql/sql_get_session_byDay.txt", "r") as f:
            myreq = f.read()

    df = extract_utils.extract_from_omniture_with_retry(myreq%dt, 10)
    print("end session %s"%dt)
    print("cleaning dataframe")
    cleaned_df = retrieve_simple_samples(df)
    cleaned_df.to_csv("data/session/%s.csv"%out_filename, sep = ";", index = False, header = True)

def extract_purchase_byDay(dt):
    out_filename = "purchase_%s"%dt
    print("retrieving purchase %s"%dt)
    with open("sql/sql_purchase_byDay.txt", "r") as f:
            myreq = f.read()

    df = extract_utils.extract_from_omniture_with_retry(myreq%dt, 10)
    print("end purchase %s"%dt)

    df.to_csv("data/purchase/%s.csv"%out_filename, sep = ";", index = False, header = True)

def main_go():
    date_list = time_utils.make_datelist("2015-01-01", 365)
    for dt in date_list:
        extract_session_byDay(dt)
        extract_purchase_byDay(dt)

if __name__ == "__main__":
    main_go()



