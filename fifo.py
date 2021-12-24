# %%
import pandas as pd 
import numpy as np 
import os
import re
import warnings
warnings.filterwarnings("ignore")
from datetime import date, datetime, timedelta
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from MergeDataFrameToTable import MergeDFToTable
spark = SparkSession.builder.enableHiveSupport().getOrCreate() 
import sys 

# def install(package):
#     subprocess.check_call(["pip", "install", package])

# import pip

# def install(package):
#     if hasattr(pip, 'main'):
#         pip.main(['install', package])
#     else:
#         pip._internal.main(['install', package])

# install('tqdm')

# %%time
# df = pd.read_csv('./data_down/inv_1220.csv', sep='\001')
# df.columns = [re.sub('\w+\.', '', i) for i in list(df.columns)]
# df = df.dropna(how = 'all', axis =1)
# time_cols = pd.Series(df.columns)[pd.Series(df.columns).str.lower().str.findall('date|time').apply(len)>0]
# df[time_cols] = df[time_cols].apply(lambda x: x.str.slice(0,10))
# df9 = df
# weeksize = '8'

def run_etl(env, weeksize, day_of_week):
    # path = './cxm/' 
    print("python version here:", sys.version, '\t') 
    print("=================================sysVersion%s================================"%env)
    print("list dir", os.listdir())
    """
    offline version
    """
    
    def allsundays(year):
        """
        十年, 
        """
        return pd.Series(pd.date_range(start=str(year), end=str(year+10), 
                            freq=day_of_week).strftime('%Y%m%d'))

    fridays = tuple([
        i for i in list(
            allsundays(2021)[allsundays(2021) < date.today().strftime('%Y%m%d')][-int(weeksize):])])

    print("=================================weeksize==%s================================"%weeksize)
    
        # 最近 8周, 每个周五. 这里直接用today是可以的, 因为只会找不等于的.,
    # 4 个站点. 
    sql = """
    select * from  dsc_dwd.dwd_wh_dsc_inventory_dtl_di 
    where src = 'scale'
    and ou_code in (
    'HPPXXWHWDS', 
    'MICHETCTGS',
    'COSTASHHTS',
    'HPPXXSHMGS',
    'MICHESHXCS'
    )
    and inc_day in """+ str(fridays) + """
    and usage_flag = '1'
    """
    print(sql)

    df = spark.sql(sql).select("*").toPandas()
    df = df.dropna(how = 'all', axis =1)
    time_cols = pd.Series(df.columns)[
        pd.Series(df.columns).str.lower().str.findall('date|time').apply(len)>0
        ]
    df[time_cols] = df[time_cols].apply(lambda x: x.str.slice(0,10))
    
    df
    df9 = df
    
    print("==================================read_table================================")
    print(df9.info())

    """
    global vars
    """
    code = ''
    df0 = pd.DataFrame()
    scan_len = len(df['inc_day'].unique())  # 8
    print(scan_len)

    def load_data(ou_code):
        """
        load bose data;
        所有类型的qty都要加起来哦
        只选择有多个收货日期的货物
        """
        global code, df, bose_inv, df0
        df0 = pd.DataFrame()
        df = df9
        df = df[df['ou_code'].astype(str) == ou_code]

        def fifo_fefo(df, type):
            if type == 'fifo':
                df['recived_date'] = pd.to_datetime(df['recived_date'])
                df['fifo_fefo'] = 'fifo'
            elif type == 'fefo':
                df['recived_date'] = pd.to_datetime(df['expiration_date'].str.slice(0,10))
                df['fifo_fefo'] = 'fefo'
            else: 
                pass
            return df 

        if ou_code in {'HPPXXWHWDS', 'HPPXXSHMGS'}:
            # hp wh
            print("load_data", " 'HPPXXWHWDS', 'HPPXXSHMGS' ")
            code = '(QH|27|QI)'
            df = fifo_fefo(df, 'fifo')

        elif ou_code in {'MICHETCTGS', 'MICHESHXCS'}:
            print("load_data", " 'MICHETCTGS', 'MICHESHXCS' ")
            df = df[df['expiration_date'] != '4712-12-31']
            # mich tc, rt,m  FEFO
            code = '(BLOCKED_TH|RETURN)'
            print("mich_if_if")
            df = fifo_fefo(df, 'fefo')

        elif ou_code == 'COSTASHHTS':
            # COSTASHHTS expiration_date 没有空值.
            df1 = df[df['expiration_date'] == '4712-12-31'] # fifo @# scale datetime. need redefine when other wms sys/..
            df2 = df[df['expiration_date'] != '4712-12-31'] # fefo
            df1 = fifo_fefo(df1, 'fifo')
            df2 = fifo_fefo(df2, 'fefo')
            df = pd.concat([df1, df2], axis = 0)
            code = '(blocked)'

        elif ou_code == 'SIEMESUEPS':
            pass

        # print(code)
        
        
        df = df[['wms_company_name', 'wms_warehouse_id','sku_code', 'sku_name', 'sku_desc', 'location',\
            'lock_codes', 'on_hand_qty', 'in_transit_qty','allocated_qty', 'shelf_days', 
            'recived_date','usage_flag', 'fifo_fefo','inc_day', 'ou_code']]
        df['qty'] = df['on_hand_qty']
        
        
        # 没有重复的 目前看....aaa
        print("===============================oucode :: %s================================="%df['ou_code'].unique())
        print(df['on_hand_qty'].describe())

        df = df.groupby(
            ['recived_date', 'sku_code', 'lock_codes','inc_day', 'wms_warehouse_id', 'fifo_fefo'],
            # dropna = False
            ).agg(
        {
            'qty':'sum',
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
        bose_inv['ou_code'] = ou_code
        print(bose_inv.head())
        return bose_inv, code
    
    def snapshot():
        """
        pivot table. inc_day 快照 作为 cols
        添加标记.
        """
        global df0, bose_inv
        df0 = pd.DataFrame()
        for i in bose_inv['sku_code'].unique():
            df_out = bose_inv[bose_inv['sku_code'] == i]\
                .pivot_table(columns=['inc_day'], index = 'recived_date', values=['qty']).reset_index()
            df_out['sku_code'] = i
            df0 = pd.concat([df0, df_out], axis = 0)
        try:
            df0.columns = df0.columns.get_level_values(level=1)
        except:
            pass
        print("snap_df0_column before in snap", df0.columns, "len of df0 in snap", (df0.shape))
        df0 = df0.reset_index(drop = True) # 4 
        df0 = pd.DataFrame(np.zeros([3, 4]))
        """
        添加缺失列
        """
        if len(df0.columns) == 10:
            print(list(df0.columns[0:scan_len]))
            df0.columns = list(df0.columns[0:scan_len]) + ['received_date','sku']
            print("normal process in snap::%s"%str(df0.shape))
        else:
            # pass
            print("auto fill enabled , ncol is: %s" %(10 - len(df0.columns)))
            somelen = 10 - len(df0.columns)
            df_zero = pd.DataFrame(np.zeros([df0.shape[0], somelen]))
            df0 = pd.concat([df_zero, df0], axis = 1)
            df0.columns = list(
                np.repeat(0, (somelen -1))
                ) + list(df0.columns[0:(scan_len)]) + ['received_date','sku']

        df0.head()
        df0 = df0.sort_values(['sku', 'received_date'])
        df0['mark'] = 0
        
        df0['mark'] = df0['mark'].where(
            df0.iloc[:, 0: (scan_len - 1)].isna().all(axis = 1) == False, 'new')
        df0['mark'] = df0['mark'].where(
            ~df0.iloc[:,(scan_len - 1)].isna() , 'clear')
        # fill na~
        df0 = df0.fillna(0)
        bose_inv = bose_inv.rename({'sku_code':'sku', 'recived_date':'received_date'}, axis = 1)\
            .reset_index(drop = True).drop(['inc_day', 'qty'], axis = 1)
        bose_inv = bose_inv.drop_duplicates(subset = ['sku', 'received_date', 'lock_codes'])
        df0 = df0.merge(bose_inv, on = ['sku','received_date'], how = 'left')

        # may lock
        df0['mark'] = df0['mark'].where(df0.iloc[:, 0:scan_len].fillna(0).nunique(axis = 1) > 1, 'may_lock')
        print("===============================snap!done for : %s=================================" %df0['ou_code'].unique())

    # print("===============================mid_function_check=================================")
    def err_part():
        """
        findout who are the naught peach.
        err 中干掉了 new 干掉了maylock 
        """
        global df0
        print("===============================err_part!start: %s================================="%str(df0.shape))
        
        df_err = df0[df0['mark'] != 'new']
        # 补充可能被锁的标记
        # df_err['mark'] = df_err['mark'].where(df_err.iloc[:, 0:8].fillna(0).nunique(axis = 1) > 1, 'may_lock')
        # 干掉了maylock
        df_err = df_err[df_err['mark'] != 'may_lock'].sort_values(['sku', 'received_date'])
        # print("===============================scan_len_err_function--%s================================="%scan_len)
        # print(df_err.iloc[:,0:scan_len])
        df_err['change'] = df_err.iloc[:,0:scan_len].diff(axis = 1).sum(axis = 1)
        shift = df_err.groupby(['sku', 'wms_warehouse_id']).shift(1) 
        shift = shift[['mark','change']]
        shift.columns = ['lag_mark', 'lag_change']
        shift['lag_mark'] = shift['lag_mark'].where(~shift['lag_mark'].isna(), 'clear')
        df_err = pd.concat([df_err, shift], axis = 1)
        print("===============================err_part!done ::%s================================="%str(df_err.shape))
        return df_err

    def output(df_err):
        global df0
        print("===============================output!start::%s================================="%str(df0.shape))

        dishes = list(df_err[(df_err['lag_mark'] != 'clear') \
            & (df_err['change'] < 0)
            & (df_err['change'] != df_err['lag_change'])]['sku'].unique())
        print("===============================output!done::%s================================="%str((df0[df0['sku'].isin(dishes)]).shape))

        return df0[df0['sku'].isin(dishes)]

    def check(sku):
        global df0
        a = df0[df0['sku'].isin(sku)].sort_values(['sku','received_date'])
        print("===============================check!done::%s================================="%str(a.shape))

        return a 

    def ou_level_lock_codes(lock_code_to_eliminate):
        """
        正则. lock_code 需要被排除的, 依赖view表格. 
        """ 
        print("===========================ou_level_lock_codes!start!code :: %s============================="%str(code))
        select_none_lock  = pd.DataFrame(
            view.groupby('sku')[
                'mark'
                ].apply(list).astype(str).str.match('.+may')
            ).reset_index()

        select_none_lock2 = pd.DataFrame(
            view.groupby('sku')[
                'lock_codes'
                ].apply(list).astype(str).str.match('.+'+lock_code_to_eliminate)
            ).reset_index()
        # 去重    
        bose_err_list = set(select_none_lock[~select_none_lock['mark']]['sku'].unique())
        bose_err_list2 = list(select_none_lock2[~select_none_lock2['lock_codes']]['sku'].unique())
        bose_err_list = list(bose_err_list.intersection(bose_err_list2))
        bose_err_list = list(set(bose_err_list))
        print("===========================ou_level_lock_codes!done :: %s==============================="%str(bose_err_list))

        return bose_err_list
    
    
    # def printt(df):
    #     print('{note:=>50}'.format(note="shape") + '{note:=>50}'.format(note=df.shape))
    #     print(df.info())

    # # df.columns
    
    out_df = pd.DataFrame()
    for ou_code0 in df9['ou_code'].unique():

        bose_inv = load_data(ou_code0)[0]
        code = load_data(ou_code0)[1]
        
        print('{note:=>50}'.format(note=ou_code0) + '{note:=>50}'.format(note=''))
        print("===============================this_code: %s================================="%ou_code0)
        print("===============================this_code_lock_code: %s================================="%code)


        print("============================boseInv before snap==============================")
        print(bose_inv.info())
        snapshot()
        print(df0.info())
        df_err = err_part()

        view = output(df_err)
        bose_err_list = ou_level_lock_codes(code)
        bose_definite_wrong = check(bose_err_list)
        print("===============================~definite_wrong~=================================")
        print(bose_definite_wrong.info())
        out_df = pd.concat([out_df, bose_definite_wrong], axis = 0)
        print(out_df.shape)
 
    print("===============================~loop_done~=================================")
    print(out_df.shape)
    print("===============================~'out_df.columns'~=================================")

    out_df['start_week'] = out_df.columns[0]
    out_df.columns = [
        str(j) + '_' + str(i) for i,j in enumerate(np.repeat('week', scan_len))
        ] + [
        'received_date','sku','mark','lock_codes',
        'wms_warehouse_id','fifo_fefo','location','ou_code', 'start_of_week'
    ]
    out_df['inc_day'] = df9['inc_day'].max()
    out_df['location'] = [','.join(i) for i in out_df['location'] ]
    out_df['received_date'] = out_df['received_date'].astype(str)


    out_df.columns
 

    print("===============================dfout_prepared=================================")

    print(out_df.columns, '\t', out_df.info())
    df = pd.DataFrame(out_df)
    df = df[['week_0',
            'week_1',
            'week_2',
            'week_3',
            'week_4',
            'week_5',
            'week_6',
            'week_7',
            'received_date',
            'sku',
            'mark',
            'lock_codes',
            'wms_warehouse_id',
            'fifo_fefo',
            'location',
            'ou_code',
            'start_of_week',
            'inc_day',]]

    df[['week_0',
            'week_1',
            'week_2',
            'week_3',
            'week_4',
            'week_5',
            'week_6',
            'week_7',]] = df[['week_0',
            'week_1',
            'week_2',
            'week_3',
            'week_4',
            'week_5',
            'week_6',
            'week_7',]].astype(float)
    df[['received_date',
            'sku',
            'mark',
            'lock_codes',
            'wms_warehouse_id',
            'fifo_fefo',
            'location',
            'ou_code',
            'start_of_week',
            'inc_day',]] = df[['received_date',
            'sku',
            'mark',
            'lock_codes',
            'wms_warehouse_id',
            'fifo_fefo',
            'location',
            'ou_code',
            'start_of_week',
            'inc_day',]].astype(str)

        


    """
    to bdp
    """
    # pd to spark table
    print('===============================0=================================')
    spark_df = spark.createDataFrame(df)
    # spark table as view, aka in to spark env. able to be selected or run by spark sql in the following part.
    spark_df.createOrReplaceTempView("df")
    # 
    print(env)

    """
    merge table preparation:
    """
   

    merge_table = "dm_dsc_ads.ads_dsc_wh_fifo_alert_wi"
    if env == 'dev':
        merge_table = 'tmp_dsc_dws.dws_dsc_wh_fifo_alert_wi'
    else:
        pass
    print('看一下merge_table from john')
    print(merge_table)
    
    inc_df = spark.sql("""select * from df""")
    print("===============================merge_table--%s================================="%merge_table)
    
    print('{note:=>50}'.format(note=merge_table) + '{note:=>50}'.format(note=''))

    spark.sql("""set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict""")
    # (table_name, df, pk_cols, order_cols, partition_cols=None):
    merge_data = MergeDFToTable(merge_table, inc_df, \
        "received_date, sku, ou_code, inc_day", "inc_day", partition_cols="inc_day")
    merge_data.merge()



def main():
    args = argparse.ArgumentParser() 
    args.add_argument("--env", help="dev environment or prod environment", default=["dev"], nargs="*")
    args.add_argument("--weeksize", help="how many weeks are we scanning, this is unmutable!!!", default=["8"], nargs="*")
    args.add_argument("--day_of_week", help="day_of_week, in picking our days", default=["W-FRI"], nargs="*")

    args_parse = args.parse_args() 
    args_parse
    env = args_parse.env [0]
    weeksize = args_parse.weeksize[0]
    day_of_week = args_parse.day_of_week [0]
    print(env, day_of_week)
    run_etl(env, weeksize, day_of_week)

    
if __name__ == '__main__':
    main()

    
# %%
# 'week_0',              954 non-null float64
# 'week_1',              954 non-null float64
# 'week_2',              954 non-null float64
# 'week_3',              954 non-null float64
# 'week_4',              954 non-null float64
# 'week_5',              954 non-null float64
# 'week_6',              954 non-null float64
# 'week_7',              954 non-null float64
# 'received_date',       954 non-null object
# 'sku',                 954 non-null object
# 'mark',                954 non-null object
# 'lock_codes',          954 non-null object
# 'wms_warehouse_id',    954 non-null object
# 'fifo_fefo',           954 non-null object
# 'location',            954 non-null object
# 'ou_code',             954 non-null object
# 'start_of_week',       954 non-null object
# 'inc_day',             954 non-null object



# week_0       double
# week_1       double
# week_2       double
# week_3       double
# week_4       double
# week_5       double
# week_6       double
# week_7       double
# received_date        string
# sku      string
# mark         string
# lock_codes       string
# wms_warehouse_id         string
# fifo_fefo        string
# location         string
# ou_code      string
# start_of_week        string