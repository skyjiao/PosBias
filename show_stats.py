# -*- coding: utf-8 -*-
__docformat__ = 'NumPy'

"""
some analysis
"""
from collections import defaultdict
from utils import time_utils, logging_utils

import pandas as pd
import numpy as np

def main_create_stat_file(begin, nb):
    date_list = time_utils.make_datelist(begin, nb)
    out_suffix = "%s_%s"%(date_list[0], date_list[-1])
    ranking_dict = defaultdict(lambda: defaultdict(int))
    counting_dict = defaultdict(lambda: defaultdict(int))
    for dt in date_list:
        print("running %s"%dt)
        df_session = pd.read_csv("data/session/session_reduced_%s.csv"%dt, sep=";")
        df_purchase = pd.read_csv("data/purchase/purchase_%s.csv"%dt, sep=";")

        session_dict = {}
        for my_id, search_id, prop60 in df_session.values:
            key = (my_id, search_id)
            session_dict[key] = prop60.split(":")
            i = 0
            for product_id in prop60.split(":"):
                counting_dict[(search_id, product_id)][i]+=1
                i += 1

        for my_id, search_id, product_id_buy in df_purchase.values:
            key = (my_id, search_id)
            if key in session_dict:
                top6 = session_dict[key]
                if product_id_buy in top6:
                    pos = top6.index(product_id_buy)
                    ranking_dict[(search_id, product_id_buy)][pos] += 1
                else:
                    ranking_dict[(search_id, product_id_buy)][100] += 1

    out = []
    for key, mydict in ranking_dict.items():
        if len(mydict) > 1:
            for k, v in mydict.items():
                if key in counting_dict:
                    out.append((key[0], key[1], k, v, counting_dict[key][k]))
    out_df = pd.DataFrame(out, columns=['search_id', 'product_id', 'position', 'purchase_count', 'display_count'])
    out_df.sort_values(['search_id', 'product_id', 'display_count'])
    out_df.to_csv("data/position_stat_%s.csv"%out_suffix, index=False, header=True, sep=";")

if __name__ == "__main__":
    main_create_stat_file(begin = "2016-01-01", nb = 30)