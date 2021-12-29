# %%
import pandas as pd 
import numpy as np 
import warnings
warnings.filterwarnings("ignore")
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from MergeDataFrameToTable import MergeDFToTable
spark = SparkSession.builder.enableHiveSupport().getOrCreate() 
import sys 
import os
from datetime import date 

def run_etl(env, day_of_week):
    # path = './cxm/' 
    print("python version here:", sys.version, '\t') 
    print("=================================sysVersion%s================================"%env)
    print("list dir", os.listdir())
    def allsundays(year):
        """
        十年, 
        """
        return pd.Series(pd.date_range(start=str(year), end=str(year+10), 
                            freq=day_of_week).strftime('%Y%m%d'))

    fridays = tuple([
        i for i in list(
            allsundays(2021)[allsundays(2021) < date.today().strftime('%Y%m%d')][-int(1):])])[0]

    """
    offline version
    """

# %%
# df = pd.read_csv('./data_down/1223_fifo_2wk.csv', sep = '\001')
# df.columns = pd.Series(df.columns).str.replace('.+\.', '')
# df2 =  pd.read_csv('./data_down/fifo_out_2w.csv', sep = '\001')
# df2.columns = pd.Series(df2.columns).str.replace('.+\.', '')

    sql = """
    select * from  dsc_dwd.dwd_wh_dsc_inventory_dtl_di 
    where  
    and inc_day = 
    and usage_flag = '1'
    """
    print(sql)

    df = spark.sql(sql).select("*").toPandas()
    df = df.dropna(how = 'all', axis =1)
    time_cols = pd.Series(df.columns)[
        pd.Series(df.columns).str.lower().str.findall('date|time').apply(len)>0
        ]
    df[time_cols] = df[time_cols].apply(lambda x: x.str.slice(0,10))
    df9 = df
    
    print("==================================read_table================================")
    print(df9.info())
    # %%
    def data_(stats):
        fefo = df[df['fifo_fefo'] == stats].sort_values(['sku' , 'received_date' ], ascending=True)
        fefo = fefo[fefo['week_7']!=0]
        fefo['out_amt'] = fefo['week_6'] - fefo['week_7']


        fefo['flag'] = fefo.groupby(
            ['sku', 'ou_code', 'wms_warehouse_id',   'fifo_fefo', 'start_of_week', 'end_of_week']
            )['sku'].transform('count')
        fefo = fefo[fefo['flag'] != 1]

        fefo['flag'] = fefo.groupby(
            ['sku', 'ou_code', 'wms_warehouse_id',   'fifo_fefo', 'start_of_week', 'end_of_week']
            )['out_amt'].transform(min)
        fefo = fefo[fefo['flag'] > 0]
        fefo.tail(8)
        return fefo
    def data2_(stats, df):
        fefo = df
        if stats == 'fifo':
            fefo = fefo.sort_values(['sku' , 'received_date'], ascending=True)
        else:
            fefo = fefo.sort_values(['sku' , 'received_date'], ascending=True)
        fefo_out = fefo.groupby(
            ['ou_code', 'wms_warehouse_id', 'fifo_fefo', 'start_of_week', 'end_of_week', 'sku'] , as_index= False
            ).agg(
                old_remain_amt = ('week_7', 'first'), # old remain
                old_remain_location = ('location', 'first'), # 易过期
                old_remain_date = ('received_date', 'first'), 
                old_remain_lock_code = ('lock_codes', 'first'), 

                new_export_amt = ('out_amt', 'mean'),  # new export
                new_export_location = ('location', 'last'),
                new_export_date = ('received_date', 'last'), 
                new_export_lock_codes = ('lock_codes', 'first'), 
        )
        return fefo_out

    fifo = data_('fifo')
    fefo = data_('fefo')
 

    # %%
    df_out = pd.concat([
        data2_('fefo', fefo), data2_('fifo', fifo)
        ], axis = 0)\
            # .to_csv('./data_up/fifo_fefo_alert_thur.csv', index = None, encoding = 'utf_8_sig')

    # %%
     




def main():
    args = argparse.ArgumentParser() 
    args.add_argument(
        "--env", help="dev environment or prod environment", default=["dev"], nargs="*")
    args.add_argument(
        "--day_of_week", help="day_of_week, in picking our days", default=["W-FRI"], nargs="*")

    args_parse = args.parse_args() 
    args_parse
    env = args_parse.env [0] 
    day_of_week = args_parse.day_of_week [0]
    print(env, day_of_week, "arguements_passed")
    run_etl(env, day_of_week)

    
if __name__ == '__main__':
    main()
