# -*- coding: utf-8 -*-
__docformat__ = 'NumPy'
'''
This module is for data extracting
'''

import time
import pyodbc
import pandas as pd
import pandas.io.sql as psql
import pdb
import re

def extract_from_omniture(req):
    """
    Params:
    ------
    req: str
        sql request

    Returns:
    -------
    out: pandas dataframe
        sql result as dataframe
    """
    t1 = time.time()
    conn = pyodbc.connect('DSN=NeoHive',autocommit=True)
    try:
        df = psql.read_sql(req, conn, coerce_float = False)
    finally:
        conn.close()
    t2 = time.time()
    print("Time spent: %d mins, %d secs\n"%(divmod(t2 - t1, 60)))
    return df

def extract_from_omniture_with_retry(req, nb = 10):
    """
    extract nb times if fails

    Parameters
    ----------
    req
    nb

    Returns
    -------

    """
    t0 = time.time()
    flag = False
    for i in range(nb):
        conn = pyodbc.connect('DSN=NeoHive',autocommit=True)
        try:
            df = psql.read_sql(req, conn, coerce_float = False)
            flag = True
        finally:
            conn.close()

        if flag:
            print("Time spent: %d mins, %d secs\n"%(divmod(time.time() - t0, 60)))
            return df
        else:
            print("attemps %d fails, will be relaunched after 30 secs"%i)
            time.sleep(30)

    print("extraction failed after %d attemps"%nb)




