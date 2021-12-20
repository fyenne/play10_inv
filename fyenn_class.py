import pandas as pd
import re
import numpy as np
from datetime import datetime, timedelta
from pandas.core.frame import DataFrame 



class pd_loaddata:
    def __init__(self, df, path, num1, num2):
        self.df = df
        self.path = path
        self.num1 = num1
        self.num2 = num2

    
    def pd_csv(path):
        """
        load data with \001 type seperator
        """
        return pd.read_csv(path, sep = '\001').dropna(axis=1, how = 'all')
        
    def pd_excel(path, num1):
        """
        doc name without format, sheet name
        """
        try:
            pd.read_csv(path + '.csv').dropna(axis=1, how = 'all')
            print('csv_read')
        except:
            try:
                pd.read_excel(path + '.xlsx', sheet_name=num1).dropna(axis=1, how = 'all')\
                    .to_csv(path + '.csv', encoding = 'utf_8_sig', index = None)
            except:
                pd.read_excel(path + '.xls', sheet_name=num1).dropna(axis=1, how = 'all')\
                    .to_csv(path + '.csv', encoding = 'utf_8_sig', index = None)
            else:
                print('excel_writed')

        return pd.read_csv(path + '.csv')


    def bdp_col(df):
        """
        colnames change to plain
        """
        df.columns = [re.sub('\w+\.', '', i) for i in list(df.columns)]
        return df 

    def pd_tocsv(df, path):
        """
        no index. utf-8
        """
        df.to_csv(path, encoding = 'utf_8_sig', index = None)

    def pd_show(num1, num2):
        """
        nrow, ncol
        """
        pd.set_option("display.max_rows", num1, "display.max_columns", num2)
    
 

# import seaborn as sns
# import plotly.express as px
# import plotly.graph_objs as go 

 