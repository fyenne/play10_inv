# %%
import pandas as pd 
import numpy as np 
import os
import re
import warnings
warnings.filterwarnings("ignore")
from datetime import date, datetime
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from MergeDataFrameToTable import MergeDFToTable
spark = SparkSession.builder.enableHiveSupport().getOrCreate() 
 
import sys 



def run_etl(start_date, end_date ,env):
    # path = './cxm/'
    print("python version here:", sys.version, '\t') 
    print("=================================sysVersion================================")
    print("list dir", os.listdir())
    """
    offline version
    """
    sql = 
    # -- select * from  dsc_dwd.dwd_wh_dsc_inventory_dtl_di 
    # -- where src = 'scale'
    and ou_code in (
    'HPPXXWHWDS', 
    'MICHETCTGS'
    'COSTASHHTS',
    'ZEBRASHALS')
    # -- and inc_day in ('20211124', '20211117', '20211110', '20211103', 
    # -- '20211027', '20211020', '20211013','20211006')
    # -- and usage_flag = '1'
    print(sql)
    df = spark.sql(sql).select("*").toPandas()
    print("==================================read_table================================")
    print(df.head())



    for ou_code0 in df['ou_code'].unique():
        
        

        def load_data(ou_code):
            """
            load bose data;
            所有类型的qty都要加起来哦
            只选择有多个收货日期的货物
            """
            global code
            df = df9
            df = df[df['ou_code'].astype(str) == ou_code]

            def fifo_fefo(df, type):
                if type == 'fifo':
                    df['recived_date'] = pd.to_datetime(df['recived_date'])
                    df['fifo_fefo'] = 'fifo'
                elif type == 'fefo':
                    df['recived_date'] = pd.to_datetime(df['expiration_date'])
                    df['fifo_fefo'] = 'fefo'
                else: 
                    pass
                return df 

            if ou_code == 'HPPXXWHWDS':
                # hp wh
                code = '(QH|27|QI)'
                df = fifo_fefo(df, 'fifo')

            elif ou_code == 'MICHETCTGS':
                # mich tc, rt,m  FEFO
                df = fifo_fefo(df, 'fefo')

            elif ou_code == 'COSTASHHTS':
                # COSTASHHTS expiration_date 没有空值.
                df1 = df[df['expiration_date'] == '4712-12-31 00:00:00'] # fifo
                df2 = df[df['expiration_date'] != '4712-12-31 00:00:00'] # fefo
                df1 = fifo_fefo(df1, 'fifo')
                df2 = fifo_fefo(df2, 'fefo')
                df = pd.concat([df1, df2], axis = 0)

            elif ou_code == 'SIEMESUEPS':
                pass
            
            
            df = df[['wms_company_name', 'wms_warehouse_id','sku_code', 'sku_name', 'sku_desc', 'location',\
                'lock_codes', 'on_hand_qty', 'in_transit_qty','allocated_qty', 'shelf_days', 
                'recived_date','usage_flag', 'fifo_fefo','inc_day']]
            # qty = pd.Series(df.columns).str.extract('(.+qty)').dropna(axis = 0)
            # df['qty'] = df[qty[0]].sum(axis = 1).round(2)
            df['qty'] = df['on_hand_qty']
            
            # bose_inv.drop_duplicates(subset = ['sku_code', 'recived_date', 'lock_codes', 'inc_day', 'qty'])
            # 没有重复的 目前看....aaa
            df = df.groupby(
                ['recived_date', 'sku_code', 'lock_codes','inc_day', 'wms_warehouse_id', 'fifo_fefo'], dropna = False
                ).agg(
            {
                'qty':sum,
                'location': set
            }
            ).sort_values(['sku_code', 'recived_date']).reset_index()
            # 只选择有多个收货日期的货物
            filter0 = df.groupby(['sku_code'])['recived_date'].agg(
            {
                set
            }
                ).reset_index()

            filter0 = pd.DataFrame(filter0[filter0['set'].apply(len)> 1]['sku_code'].drop_duplicates())
            bose_inv = filter0.merge(df, on = ['sku_code'], how = 'inner')\
                .sort_values(['recived_date','sku_code', 'inc_day'])
            
            return bose_inv
        
        bose_inv = load_data(ou_code0)
        # for loop needed or use group by.

        df0 = pd.DataFrame()
        def snapshot():
            """
            pivot table. inc_day 快照 作为 cols
            添加标记.
            """
            global df0, bose_inv
            for i in bose_inv['sku_code'].unique():
                df_out = bose_inv[bose_inv['sku_code'] == i]\
                    .pivot_table(columns=['inc_day'], index = 'recived_date', values=['qty']).reset_index()
                df_out['sku_code'] = i
                df0 = pd.concat([df0, df_out], axis = 0)
            try:
                df0.columns = df0.columns.get_level_values(level=1)
            except:
                pass
            df0.columns = list(df0.columns[0:8]) + ['received_date','sku']
            df0 = df0.sort_values(['sku', 'received_date'])
            df0['mark'] = 0
            
            df0['mark'] = df0['mark'].where(df0.iloc[:, 0:7].isna().all(axis = 1) == False, 'new')
            df0['mark'] = df0['mark'].where(~df0.iloc[:,7].isna() , 'clear')
            # fill na~
            df0 = df0.fillna(0)
            bose_inv = bose_inv.rename({'sku_code':'sku', 'recived_date':'received_date'}, axis = 1)\
                .reset_index(drop = True).drop(['inc_day', 'qty'], axis = 1)
            bose_inv = bose_inv.drop_duplicates(subset = ['sku', 'received_date', 'lock_codes'])
            df0 = df0.merge(bose_inv, on = ['sku','received_date'], how = 'left')

            # may lock
            df0['mark'] = df0['mark'].where(df0.iloc[:, 0:8].fillna(0).nunique(axis = 1) > 1, 'may_lock')
        
        snapshot()


        def err_part():
            """
            findout who are the naught peach.
            err 中干掉了 new 干掉了maylock 
            """
            df_err = df0[df0['mark'] != 'new']
            # 补充可能被锁的标记
            # df_err['mark'] = df_err['mark'].where(df_err.iloc[:, 0:8].fillna(0).nunique(axis = 1) > 1, 'may_lock')
            # 干掉了maylock
            df_err = df_err[df_err['mark'] != 'may_lock'].sort_values(['sku', 'received_date'])
            df_err['change'] = df_err.iloc[:,0:8].diff(axis = 1).sum(axis = 1)
            shift = df_err.groupby(['sku', 'wms_warehouse_id']).shift(1) 
            shift = shift[['mark','change']]
            shift.columns = ['lag_mark', 'lag_change']
            shift['lag_mark'] = shift['lag_mark'].where(~shift['lag_mark'].isna(), 'clear')
            df_err = pd.concat([df_err, shift], axis = 1)
            return df_err
        
        df_err = err_part()
        def output(df_err, df0):
            dishes = list(df_err[(df_err['lag_mark'] != 'clear') \
                & (df_err['change'] < 0)
                & (df_err['change'] != df_err['lag_change'])]['sku'].unique())
            return df0[df0['sku'].isin(dishes)]
    
        def check(sku, df0):
            a = df0[df0['sku'].isin(sku)].sort_values(['sku','received_date'])
            return a 

        view = output(df_err, df0)

        def ou_level_lock_codes(lock_code_to_eliminate):
            """
            正则. lock_code 需要被排除的, 依赖view表格. 
            """
            # output(df_err_bose,df0_bose)['sku'].unique()
            select_none_lock  = pd.DataFrame(
                view.groupby('sku')['mark'].apply(list).astype(str).str.match('.+may')
                ).reset_index()
            select_none_lock2 = pd.DataFrame(
                view.groupby('sku')['lock_codes'].apply(list).astype(str).str.match('.+'+lock_code_to_eliminate)
                ).reset_index()
            # 去重    
            bose_err_list = set(select_none_lock[~select_none_lock['mark']]['sku'].unique())
            bose_err_list2 = list(select_none_lock2[~select_none_lock2['lock_codes']]['sku'].unique())
            bose_err_list = list(bose_err_list.intersection(bose_err_list2))
            bose_err_list = list(set(bose_err_list))
            return bose_err_list



        bose_err_list = ou_level_lock_codes(code)
        bose_definite_wrong = check(bose_err_list, df0)
        
    # .to_csv('./data_up/bose_fifo.csv', index = None, encoding = 'utf_8_sig')


    """
    merge table preparation:
    """
   

    # merge_table = "dsc_dws.dws_dsc_huawei_operation_sum_df"
    # if env == 'dev':
    #     merge_table = 'tmp_' + merge_table
    # else:
    #     pass
    # print('看一下merge_table from john')
    # print(merge_table)
    
    # inc_df = spark.sql("""select * from df""")
    # print("===============================merge_table--%s================================="%merge_table)
    # # merge_table = "tmp_dsc_dws.dws_dsc_huawei_operation_sum_df"
    # # print(merge_table)
    # print('{note:=>50}'.format(note=merge_table) + '{note:=>50}'.format(note=''))

    # spark.sql("""set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict""")
    # # (table_name, df, pk_cols, order_cols, partition_cols=None):
    # merge_data = MergeDFToTable(merge_table, inc_df, \
    #     "ou, update_date, inc_day", "inc_day", partition_cols="inc_day")
    # merge_data.merge()



def main():
    args = argparse.ArgumentParser()
    args.add_argument("--start_date", help="start date for refresh data, format: yyyyMMdd"
                          , default=[(datetime.now()).strftime("%Y%m%d")], nargs="*")

    args.add_argument("--env", help="dev environment or prod environment", default="dev", nargs="*")

    args_parse = args.parse_args()
    start_date = args_parse.start_date[0]
    env = args_parse.env[0]
 
    run_etl(start_date, env)

    
if __name__ == '__main__':
    main()

    