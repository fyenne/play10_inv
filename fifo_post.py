# %%
import pandas as pd 
import numpy as np 
from datetime import date 
import warnings
warnings.filterwarnings("ignore")
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from MergeDataFrameToTable import MergeDFToTable
spark = SparkSession.builder.enableHiveSupport().getOrCreate() 
import sys 
import os

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
# day_of_week = 'W-FRI'
# df = pd.read_csv('./data_down/1223_fifo_2wk.csv', sep = '\001')
# df.columns = pd.Series(df.columns).str.replace('.+\.', '')
# df2 =  pd.read_csv('./data_down/fifo_out_2w.csv', sep = '\001')
# df2.columns = pd.Series(df2.columns).str.replace('.+\.', '')

    sql = """
    select * from  dm_dsc_ads.ads_dsc_wh_fifo_alert_wi_dtl
    where  weeksize = '2'
    and inc_day = '""" + str(fridays) + "'"
    print(sql)

    df = spark.sql(sql).select("*").toPandas()
    
    print("==================================read_table================================")
    print(df.info())
    # %%
    def data_(stats):
        """
        对数据筛选分类
        """
        fefo = df[df['fifo_fefo'] == stats].sort_values(['sku' , 'received_date' ], ascending=True)
        fefo = fefo[fefo['week_7']!=0]
        fefo['out_amt'] = fefo['week_6'] - fefo['week_7']


        fefo['flag'] = fefo.groupby(
            ['sku', 'ou_code', 'wms_warehouse_id',   'fifo_fefo', 'start_of_week', 'end_of_week']
            )['sku'].transform('count')
        fefo = fefo[fefo['flag'] != 1]

        fefo['flag'] = fefo.groupby(
            ['sku', 'ou_code', 'wms_warehouse_id',   'fifo_fefo', 'start_of_week', 'end_of_week']
            )['out_amt'].transform('min')
        fefo = fefo[fefo['flag'] > 0]
        fefo.tail(8)
        return fefo

    def data2_(stats, df):
        fefo = df
        if stats == 'fifo':
            fefo = fefo.sort_values(['sku' , 'received_date'], ascending=True)
        else:
            fefo = fefo.sort_values(['sku' , 'received_date'], ascending=True) 
            # ? 搞不懂发生了什么. 但这样的结果才是正确的..
        fefo_out = fefo.groupby(
            ['ou_code', 'wms_warehouse_id', 'fifo_fefo', 'start_of_week', 'end_of_week', 'sku'],
            as_index= False
        #     ).agg(
        #         old_remain_amt = ('week_7', 'first'), # old remain
        #         old_remain_location = ('location', 'first'), # 易过期
        #         old_remain_date = ('received_date', 'first'), 
        #         old_remain_lock_code = ('lock_codes', 'first'), 

        #         new_export_amt = ('out_amt', 'mean'),  # new export
        #         new_export_location = ('location', 'last'),
        #         new_export_date = ('received_date', 'last'), 
        #         new_export_lock_codes = ('lock_codes', 'first'), 
        # )   # version failed.
            ).agg({
                'week_7' : [('old_remain_amt', 'first')], 
                'out_amt' : [('new_export_amt', 'first')], 
                'location': [('old_remain_location', 'first'), ('new_export_location', 'last')], 
                'received_date': [('old_remain_date','first'), ('new_export_date', 'last')],
                'lock_codes':[('old_remain_lock_code','first'), ('new_export_lock_codes', 'last')],
                }
        )
        return fefo_out
    
    # %%
    fifo = data_('fifo')
    fefo = data_('fefo')
    print("=================================0=================================")
    
    df = pd.concat([
        data2_('fefo', fefo), data2_('fifo', fifo)
        ], axis = 0)\
            # .to_csv('./data_up/fifo_fefo_alert_thur.csv', index = None, encoding = 'utf_8_sig')
    df.columns = list(df.columns.get_level_values(0)[0:6]) \
        + list(df.columns.get_level_values(1)[6:])
    print(df.columns)
    df['inc_day'] = str(fridays)
    
    
    df[[
        'ou_code', 
        'wms_warehouse_id', 
        'fifo_fefo', 
        'start_of_week', 
        'end_of_week', 
        'sku', 
        # 'old_remain_amt', 
        'old_remain_location', 
        'old_remain_date', 
        'old_remain_lock_code', 
        'new_export_amt', 
        'new_export_location', 
        'new_export_date', 
        'new_export_lock_codes', 
        'inc_day'
        ]] = df[[
        'ou_code', 
        'wms_warehouse_id', 
        'fifo_fefo', 
        'start_of_week', 
        'end_of_week', 
        'sku', 
        # 'old_remain_amt', 
        'old_remain_location', 
        'old_remain_date', 
        'old_remain_lock_code', 
        'new_export_amt', 
        'new_export_location', 
        'new_export_date', 
        'new_export_lock_codes', 
        'inc_day'
        ]].astype(str)

    df[[
        'old_remain_amt', 
        'new_export_amt', 
        ]] = df[[
        'old_remain_amt', 
        'new_export_amt', 
        ]].astype(float)
    df = df[[
        'ou_code', 
        'wms_warehouse_id', 
        'fifo_fefo', 
        'start_of_week', 
        'end_of_week', 
        'sku', 
        'old_remain_amt', 
        'old_remain_location', 
        'old_remain_date', 
        'old_remain_lock_code', 
        'new_export_amt', 
        'new_export_location', 
        'new_export_date', 
        'new_export_lock_codes', 
        'inc_day'
        ]]

    # %%
    # spark table.
    # spark table as view. 
    # able to be selected or run by spark sql in the following part.
    spark_df = spark.createDataFrame(df)
    spark_df.createOrReplaceTempView("df")
    # 
    print(env)

    """
    merge table preparation:
    """
    merge_table = 'dm_dsc_ads.ads_dsc_wh_fifo_alert_wi_sum'
    if env == 'dev':
        merge_table = 'tmp_' + merge_table
    else:
        pass
    # dm_dsc_ads.ads_dsc_wh_fifo_alert_wi
    print('看一下merge_table <>')
    print(merge_table)
    inc_df = spark.sql("""select * from df""")
    print(inc_df)
    print("===============================merge_table--%s================================="%merge_table)
    
    print('{note:=>50}'.format(note=merge_table) + '{note:=>50}'.format(note=''))
    spark.sql("""set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict""")
    # (table_name, df, 
    # pk_cols, order_cols, partition_cols=None):
    merge_data = MergeDFToTable(merge_table, inc_df, \
        "ou_code, sku, inc_day", "inc_day", partition_cols="inc_day")
    merge_data.merge()


def main():
    args = argparse.ArgumentParser() 
    args.add_argument(
        "--env", help="dev environment or prod environment", default=["dev"], nargs="*")
    args.add_argument(
        "--day_of_week", help="day_of_week, in picking our days", default=["W-FRI"], nargs="*")

    args_parse = args.parse_args()  
    env = args_parse.env [0] 
    day_of_week = args_parse.day_of_week [0]
    print(env, day_of_week, "arguements_passed")
    run_etl(env, day_of_week)

    
if __name__ == '__main__':
    main()
