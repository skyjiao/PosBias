# -*- coding: utf-8 -*-
__docformat__ = 'NumPy'

import numpy as np
import pandas as pd

def extract():
    sql = """
INSERT OVERWRITE TABLE search_dev.{table_name}
SELECT search_id, product_id, product_freq
FROM(
SELECT
search_id,
product_id,
count(*) as product_freq,
ROW_NUMBER() OVER(PARTITION BY search_id ORDER BY count(*) DESC)
AS rank
FROM(
SELECT search_id, split(prop60, ':') as products
FROM search.taskforce_sv_search
WHERE
dt >= '{date_from}' AND
                     dt <= '{date_until}'
             )table_products
             LATERAL VIEW explode(products) exploded_table AS product_id
           WHERE
                NOT isnull(search_id) AND                 search_id <> ''
                AND
                                not isnull(product_id) AND                  product_id <> ''
             GROUP BY search_id, product_id
         )table_products_freq
                   WHERE rank <= {max_products_per_query}
          ORDER BY search_id, product_freq desc
          """
         sql = sql.format(date_from=date_from.isoformat(),
                           date_until=date_until.isoformat(),
                           max_products_per_query=max_products_per_query,
                           table_name=table_name)


