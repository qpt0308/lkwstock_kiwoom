import multiprocessing
import time
import traceback
from datetime import datetime, timedelta
from multiprocessing import Process
from multiprocessing.pool import ThreadPool
from multiprocessing.spawn import freeze_support

import numpy as np
import pandas
from PyQt5.QtCore import QThread, pyqtSignal
from log.log_class import logger
from util.base.notifier import send_message
from util.calculation.Calculation_my import Calculation_my
from util.database.db_connecter import db_connecter

freeze_support()

class analysis_th(Process):
    def __init__(self,process_conn):
        super(analysis_th, self).__init__()
        self.process_conn = process_conn
        self.base_invest_info = self.process_conn['base_invest_info']
        self.working = True
        self.run_bool = True
        self.target_qut = 20
        self.where = ''
        self.pro_bool = True
        self.process_count =0

    def __del__(self):
        self.run_bool =False

    def db_to_df(self,params):
        try:
            if type(params) == tuple :
                table_name = params[1]
            else:
                table_name = params
            if self.target_qut != 0 :
                sql = "select * from {}{}".format(table_name,self.sqlwhere)
                low_df = self.target_db.select_db_to_df(sql)

                tt_code=str(low_df['종목코드'].values[0])
                if self.target_db.check_table_exist('buy_list') :
                    sql = f"select * from buy_list where 종목코드 = '{tt_code}' ORDER BY id DESC ;"  # 저장된 데이터의 가장 최근 일자 조회
                    cur_df = self.target_db.select_db_to_df(sql)
                else:
                    cur_df =pandas.DataFrame()

                if not cur_df.empty:
                    if low_df['체결시간'].values[0] == cur_df['체결시간'].values[0] :
                        return

                if low_df['최우선매수호가'].values[0] >= self.base_invest_info['종목당투자'] :
                    return

                aaa = low_df['체결시간'].values[0]
                now = datetime.now()
                h = aaa[0:2]
                m = aaa[2:4]
                s = aaa[4:6]
                d2 = now.replace(hour=int(h), minute=int(m), second=int(s))
                d3 = now - timedelta(minutes=2)
                if low_df['생성일자'].values[0] == datetime.now().strftime('%Y%m%d') and now >= d2 >= d3:
                        cal = Calculation_my()
                        self.pro_bool = True
                        copy_df = low_df.copy()
                        #granville_val = cal.granville_theory_buy(copy_df.copy(),'체결시간','최우선매수호가',period=2,duration=2,index_sub='생성일자')
                        copy_df = copy_df.sort_values(by=['생성일자', '체결시간'], ascending=True).copy()
                        temp_df = copy_df[['체결시간', '최우선매수호가']]
                        temp_df.set_index('체결시간', inplace=True)
                        ma_val1 =2
                        ma_val2 = 20
                        ma_val3 = 200
                        ma_dfA = pandas.DataFrame(temp_df.copy(), index=temp_df.index.astype('str')).rolling(window=ma_val1,min_periods=1).mean()
                        ma_dfB = pandas.DataFrame(temp_df.copy(), index=temp_df.index.astype('str')).rolling(window=ma_val2,min_periods=1).mean()
                        ma_dfC = pandas.DataFrame(temp_df.copy(), index=temp_df.index.astype('str')).rolling(window=ma_val3,min_periods=1).mean()
                        copy_df.set_index('체결시간', inplace=True)
                        copy_df[f'최우선매수이평{ma_val1}'] = ma_dfA.copy()
                        copy_df[f'최우선매수이평{ma_val2}'] = ma_dfB.copy()
                        copy_df[f'최우선매수이평{ma_val3}'] = ma_dfC.copy()
                        copy_df = copy_df.reset_index()
                        copy_df.rename(columns={'index': '체결시간'}, inplace=True)
                        copy_df['거래량'] = copy_df['누적거래량'].diff(1).copy()
                        copy_df = cal.rsi_db_to_df(copy_df.copy(), '거래량', 2, index_col='체결시간', sort_sub='생성일자',ris_name='거래량RSI2')
                        copy_df = cal.rsi_db_to_df(copy_df.copy(), '체결강도', 2, index_col='체결시간', sort_sub='생성일자',ris_name='체결강도RSI2')
                        copy_df = cal.rsi_db_to_df(copy_df.copy(), '체결강도', 14, index_col='체결시간', sort_sub='생성일자',ris_name='체결강도RSI14')
                        copy_df = cal.rsi_db_to_df(copy_df.copy(), '최우선매수호가', 14, index_col='체결시간', sort_sub='생성일자',ris_name='최우선매수호가RSI14')
                        copy_df = copy_df.sort_values(by=['생성일자', '체결시간'], ascending=False).copy()
                        target_code = copy_df['종목코드'].values[0]
                        plan1 = eval(f"(copy_df.최우선매수호가.values[0] >= copy_df.최우선매수이평{ma_val1}.values[0])")
                        plan2 = eval(f"(copy_df.최우선매수이평{ma_val1}.values[0] < copy_df.최우선매수이평{ma_val2}.values[0])")
                        plan3 = eval(f"(copy_df.최우선매수이평{ma_val2}.values[0] < copy_df.최우선매수이평{ma_val3}.values[0])")
                        plan4 = "True"#eval("((copy_df.거래량RSI2.values[0] == 0) and (copy_df.거래량RSI2.values[1] == 0) and (copy_df.거래량RSI2.values[2] >= 51))")
                        plan5 = "True"#eval("((copy_df.체결강도RSI2.values[0] == 0) and (copy_df.체결강도RSI2.values[1] == 0))")
                        plan6 = "True"#eval("(((copy_df.저가.values[0] * 1.02) >= copy_df.최우선매수호가.values[0]) and (copy_df.저가.values[0] != copy_df.최우선매수호가.values[0]))")
                        plan7 = eval("((copy_df.체결강도RSI14.values[0] >= 50))")
                        plan8 = "True"#eval(f"((copy_df.최우선매수호가.values[1] < copy_df.최우선매수이평{ma_val1}.values[1]) and (copy_df.최우선매수호가.values[2] < copy_df.최우선매수이평{ma_val1}.values[2]))")
                        plan9 = "True"#eval(f"((copy_df.최우선매수호가RSI14.values[1] < 50))")

                        buy_df = copy_df.head(1).copy()
                        #print(buy_df)
                        if eval("plan1") and eval("plan2") and eval("plan3") and eval("plan4") and eval("plan5") and eval('plan6') and eval('plan7') and eval('plan8') and eval('plan9'):
                                buy_df['진행상황'] = "포착"
                                this_time = datetime.now()
                                buy_df['등록시간'] = this_time
                                self.target_db.insert_df_to_db("buy_list", buy_df, option='append')
                                send_message(f"{buy_df['등록시간']}/ 종목코드 : {target_code} 매수신호 포착!!.")
                                logger.debug(f"{buy_df['등록시간']}/ 종목코드 : {target_code} 매수신호 포착!!.")

        except Exception as e:
            logger.debug(traceback.format_exc())
            send_message(f"analysis_thread : db_to_df 오류입니다.{traceback.format_exc()}")
            print(traceback.format_exc())


    def run(self):
        try:
            self.target_db = db_connecter(db_info_gubu=self.process_conn['db_info_gubu'])
            message_time=datetime.now()
            while self.run_bool :
                self.working = self.process_conn['working']
                if self.working :
                    start_temp = time.time()
                    table_df = self.target_db.check_table_exist(table_name='real_deal_date', list_return=True)
                    bef= (datetime.now() - timedelta(minutes=2))
                    this_time = np.datetime64(bef, 's')
                    table_df = table_df[(table_df['table_rows'] >= self.target_qut) & ((table_df['update_time'] >= this_time) | (table_df['create_time'] >= this_time))]
                    if self.where != "":
                        self.sqlwhere = " WHERE {} ORDER BY id DESC".format(self.where)
                    else:
                        self.sqlwhere = " ORDER BY id DESC"

                    if len(table_df.index.values) > 100:
                        pool_qut = 100
                    else:
                        pool_qut = len(table_df.index.values)
                    #print(4)
                    if pool_qut > 0:
                        with ThreadPool(pool_qut) as p:
                            manager = multiprocessing.Manager()
                            d = manager.dict()
                            p.map(self.db_to_df, [(d, temp_name) for temp_name in table_df['table_name'].values])
                            self.pro_bool = True

                    end_temp = time.time()
                    if self.pro_bool == True:
                        self.process_count = self.process_count + 1
                        logger.debug("$$분석시간$$ : {}$$ {}차 분석완료$$..!!".format(end_temp - start_temp,self.process_count))
                        self.pro_bool=False

                    if datetime.now() >= message_time:
                        message_time = datetime.now() + timedelta(minutes=1)
                        now = datetime.now()
                        send_message(f"{now} analysis_th 실행중입니다.\n 처리시간 : {end_temp - start_temp}")
                        logger.debug(f"{now} analysis_th 실행중입니다.\n 처리시간 : {end_temp - start_temp}")
                        print(f"{now} analysis_th 실행중입니다. 처리시간 : {end_temp - start_temp}")
        except KeyboardInterrupt:
            self.run_bool=False
            print(f'{self.__class__}KeyboardInterrupt')
        except Exception as e:
            logger.debug(traceback.format_exc())
            send_message(f"analysis_thread : run 오류입니다.{traceback.format_exc()}")
            print(traceback.format_exc())

