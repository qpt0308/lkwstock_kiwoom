import multiprocessing
import traceback
from multiprocessing import Process
from multiprocessing.spawn import freeze_support

import pandas
from PyQt5.QtCore import *
from multiprocessing.pool import ThreadPool
from util.calculation.Calculation_my import Calculation_my
from util.database.db_connecter import db_connecter

freeze_support()

class db_sever_to_local(QThread):

    list_changed = pyqtSignal(dict)
    chart_dict = pyqtSignal(dict)

    def __init__(self ,parent=None):
        super().__init__()
        self.main =parent
        self.cal = Calculation_my()
        self.working = True
        self.work_gubun = ""
    """
    def __del__(self):
        print("----end thread------")
        self.wait()
    """
    def thread_funtion(self, params):
        try:
            table_name = params[1]
            share_data = params[0]
            db_in = self.db_in
            db_out = self.db_out
            out_table_name = 'analy'+ table_name[-6:]
            sql_in = "select * from {}".format(table_name)
            df = db_in.select_db_to_df(sql_in)
            out_max_date = ""
            if db_out.check_table_exist(table_name=out_table_name):
                sql_out = "select max(생성일자) from {}".format(out_table_name)
                df_out_date = db_out.select_db_to_df(sql_out)
                out_max_date = df_out_date['max(생성일자)'].values[0]
            #print(df_out_date)
            in_max_date = df['생성일자'].max()

            df = df[(df['생성일자'] == in_max_date)].copy()

            if not df.empty and out_max_date != in_max_date:

                df['거래량'] = df['누적거래량'].diff(1).copy()

                temp_df = df[['체결시간', '최우선매수호가']].copy()
                temp_df2 = df[['체결시간','거래량']].copy()
                temp_df.set_index('체결시간', inplace=True)
                temp_df2.set_index('체결시간', inplace=True)
                ma_put = [2,20,200]
                ma_df = {}
                for ma in ma_put:
                    ma_df[ma] = pandas.DataFrame(temp_df.copy(), index=temp_df.index.astype('str')).rolling(window=ma,min_periods=1).mean()
                deal_qut = pandas.DataFrame(temp_df2.copy(), index=temp_df2.index.astype('str')).rolling(window=14,min_periods=1).mean()

                df.set_index('체결시간', inplace=True)

                for ma in ma_put:
                    df[f'최우선매수이평{ma}'] = ma_df[ma].copy()
                df['거래량이평14'] = deal_qut.copy()

                df = df.reset_index()

                df.rename(columns={'index': '체결시간'}, inplace=True)

                ris_dict = {"거래량" : 14,"체결강도" : 14}
                for ris_name in ris_dict.keys():
                    ris = ris_dict[ris_name]
                    df = self.cal.rsi_db_to_df(df.copy(), ris_name, ris, index_col='체결시간', sort_sub='생성일자',ris_name=f"{ris_name}RSI{ris}")
                ris_dict = {"거래량": 2, "체결강도": 2}
                for ris_name in ris_dict.keys():
                    ris = ris_dict[ris_name]
                    df = self.cal.rsi_db_to_df(df.copy(), ris_name, ris, index_col='체결시간', sort_sub='생성일자', ris_name=f"{ris_name}RSI{ris}")

                df.drop(['id'], axis=1, inplace=True)
                df = df.fillna("")
                #print(df['거래량'])
                db_out.insert_df_to_db(out_table_name, df, option='append')
            self.list_changed.emit({'statu': 'ing', 'id': "", 'name': out_table_name, 'total': params[0]['total']})

        except Exception as e:
            print(traceback.format_exc())


    def db_in_out(self):
        try:
            self.db_in = db_connecter(db_info_gubu='local')
            self.db_out = db_connecter(db_info_gubu='local', db_name="cinder")
            manager = multiprocessing.Manager()
            d = manager.dict()
            d['count'] = 0
            print('시작!!')
            table_df = self.db_in.check_table_exist(table_name='real_deal_date', list_return=True)
            if len(table_df['table_name'].values) > 100:
                pool_qut = 100
            else:
                pool_qut = len(table_df['table_name'].values)
            d['total'] = len(table_df['table_name'].values)
            with ThreadPool(pool_qut) as p:
                p.map(self.thread_funtion, [(d, table_name) for table_name in table_df['table_name'].values])
            self.list_changed.emit({'statu': 'end', 'id': "", 'name': "", 'total': ""})
            self.working=False
        except Exception as e:
            print(e)

    def run(self):
        while self.working :
            self.db_in_out()


