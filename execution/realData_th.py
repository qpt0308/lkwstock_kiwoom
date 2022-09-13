import multiprocessing
from multiprocessing import Process
from multiprocessing.pool import ThreadPool
from multiprocessing.spawn import freeze_support

import numpy as np

from imp_collection.base_import import *
from util.database.db_connecter import db_connecter

class KeyboardInterruptError(Exception) : pass
freeze_support()
class realData_th(Process):

    def __init__(self,process_conn):
        freeze_support()
        super(realData_th, self).__init__()
        self.working = True
        self.run_bool = True
        self.process_conn = process_conn
        self.base_invest_info = self.process_conn['base_invest_info']


    def __del__(self):
        self.run_bool = False

    def fa_st_Thread(self,params):
        try:
            temp_input = params[0]['temp_input'].copy()
            temp_code = params[1]
            temp_table_nmA = params[0]['table_nameA']
            before_time = ""
            te_now_date = datetime.now().strftime('%Y%m%d')
            this_time = (datetime.now() - timedelta(minutes=2))
            #print(f'갱신시간tpye : {type(temp_input[temp_code]["갱신시간"])}')
            #print(f'갱신시간tpye : {temp_input[temp_code]["갱신시간"]}')
            #print(f'now : {this_time}')
            if temp_input[temp_code]['갱신시간'] >= this_time:
                if self.target_db.check_table_exist(temp_table_nmA+temp_code):
                    if temp_input[temp_code]['최우선매수호가'] > 0:
                        sql = "select * from {} ORDER BY id DESC LIMIT 1".format(temp_table_nmA+temp_code)  # 저장된 데이터의 가장 최근 일자 조회
                        cur_df = self.target_db.select_db_to_df(sql)
                        if not cur_df.empty:
                            cur_df =cur_df.sort_values(by=['생성일자','체결시간'], ascending=False).drop_duplicates(subset=['생성일자'], keep='first').copy()
                            before_time = list(cur_df['체결시간'].values)[0]

                if temp_input[temp_code]['체결시간'] != before_time and temp_input[temp_code]['최우선매수호가'] > 0 :

                    temp_col_list = ["생성일자", "종목코드"]
                    temp_val_list = [te_now_date, temp_code]

                    for col in temp_input[temp_code].keys():
                        temp_col_list.append(col)
                        temp_val_list.append(temp_input[temp_code][col])

                    df = pandas.DataFrame(data=[temp_val_list], columns=temp_col_list)
                    df['갱신시간'] = df['갱신시간'].astype('string')
                    self.target_db.insert_df_to_db(temp_table_nmA+temp_code, df, option="append")
        except Exception as e:
            send_message(f"thread pool 샐행중오류입니다.{traceback.format_exc()}{sql}")
            logger.debug(traceback.format_exc())
            print(traceback.format_exc())

    def run(self):
        pool_qut = 0
        message_time = datetime.now()
        self.target_db = db_connecter(db_info_gubu=self.process_conn['db_info_gubu'])
        while self.run_bool :
            try:
                self.working = self.process_conn['working']
                if self.working:

                    start_temp = time.time()
                    if check_transaction_open():
                        #실시간 체결정보 3초이내 단위 db에 저장. multithreadpool 방식으로

                        temp_input = self.process_conn['real_dict'].copy()
                        #print(f"realdate : {len(temp_input)}")
                        if len(temp_input.keys()) != 0:
                            if len(temp_input.keys()) > 500:
                                pool_qut = 500
                            else:
                                pool_qut = len(temp_input.keys())

                            manager = multiprocessing.Manager()
                            d = manager.dict()
                            d['table_nameA'] = 'real_deal_date'
                            d['temp_input'] = temp_input.copy()

                            with ThreadPool(pool_qut) as p:
                                p.map(self.fa_st_Thread, [(d,temp_code) for temp_code in temp_input.keys()])


                    end_temp = time.time()
                    if datetime.now() >= message_time:
                        message_time = datetime.now() + timedelta(minutes=1)
                        now = datetime.now()
                        send_message(f"{now} {self.__class__} 실행중입니다.\n 처리시간 : {end_temp - start_temp}")
                        logger.debug(f"{now} {self.__class__} 실행중입니다.\n 처리시간 : {end_temp - start_temp}")
                        print(f"{now} {self.__class__} 실행중입니다. 처리시간 : {end_temp - start_temp}")

            except KeyboardInterrupt:
                self.run_bool=False
                print(f'{self.__class__}KeyboardInterrupt')

            except Exception as e:
                now = datetime.now()
                send_message(f"{now}{self.__class__}{traceback.format_exc()}")
                logger.debug(f"{now}{self.__class__}{traceback.format_exc()}")
                print(f"{self.__class__}traceback.format_exc()")
