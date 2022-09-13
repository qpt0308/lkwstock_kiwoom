import math
import multiprocessing
import os
import warnings
from collections import deque
from multiprocessing.spawn import freeze_support
import exchange_calendars
from PyQt5.QtCore import *
from api.Kiwoom_A import Kiwoom
from api.const import get_fid
from execution.analysis_th import analysis_th
from execution.realData_th import realData_th
from execution.realUniverse_th import realUniverse_th
from util.database.db_connecter import *
from imp_collection.base_import import *

warnings.simplefilter(action='ignore', category=FutureWarning)
freeze_support()
class execution_main(QThread):

    def __init__(self):
        QThread.__init__(self)
        self.target_db = db_connecter(db_info_gubu='local')
        self.base_invest_info = {
            '실예수금액': 0,
            '불용예수금': 0,
            '투자가능금': 500000,
            '가용예수금': 0,
            '최대보유수': 25,
            '보유종목수': 0,
            '매수접수수': 0,
            '매도접수수': 0,
            '종목당투자': 0,
            '목표수익률': 0.015
        }
        self.cost_info = {
            '수수료률': 0.00015,
            '매도세금': 0.003
        }
        self.working = True
        self.strat_bool = False
        self.queue = deque()
        self.single_disconn_bool = False
        self.is_init_succed = False
        self.order_bool = {'bool': False, 'name': ''}
        self.universe_re_dict = {}
        self.default_screen_num = 9000
        self.default_end_step = 99
        self.real_screen_num_storage = 0
        self.end_code_count = 0

        manager = multiprocessing.Manager()

        self.uni_data_conn = manager.dict()
        self.real_data_conn = manager.dict()
        self.analysis_data_conn = manager.dict()

        self.uni_data_conn['db_info_gubu'] = self.target_db.db_info_gubu
        self.real_data_conn['db_info_gubu'] = self.target_db.db_info_gubu
        self.analysis_data_conn['db_info_gubu'] = self.target_db.db_info_gubu

        xkrx = exchange_calendars.get_calendar('XKRX')
        self.trading_day = xkrx.is_session(datetime.now().strftime('%Y-%m-%d'))  # 오늘이 개장일인지 확인.
        # xkrx.next_open(pd.Timestamp.today()) #다음 개장일은 언제인지 확인

        if self.trading_day:
            print(f'{datetime.now()}오늘은 영업입니다..!!')
            self.init_main()
        else:
            print(f'{datetime.now()}오늘은 영업이 아닙니다..!!')


    def __del__(self):
        pass

    def init_main(self): # 초기화 합수.

        self.kiwoom = Kiwoom()

        self.exist_deposit()

        self.base_invest_info_set()

        self.rU_th = realUniverse_th(self.uni_data_conn)

        self.set_universe_real_time()

        self.analy_th = analysis_th(self.analysis_data_conn)

        self.rD_th = realData_th(self.real_data_conn)

        self.real_data_conn['working'] = False
        self.analysis_data_conn['working'] = False

        self.rU_th.start()
        self.rD_th.start()
        self.analy_th.start()

        self.is_init_succed = True

    def base_invest_info_set(self):
        self.uni_data_conn['base_invest_info'] = self.base_invest_info
        self.real_data_conn['base_invest_info'] = self.base_invest_info
        self.analysis_data_conn['base_invest_info'] = self.base_invest_info

    def universe_seting(self,table_name):

        temp_df = pandas.DataFrame()

        if self.target_db.check_table_exist(table_name=table_name):
            sql = f"select * from {table_name}"  # 유니버스라는 테이블 내용을 모두 가지고 온다.
            temp_df = self.target_db.select_db_to_df(sql)  # db_helper.py에 select_sql 함수 호출 Dataframe 스타일로 반환

        return temp_df

    def real_deal_table_del(self):
        table_df = self.target_db.check_table_exist(table_name='real_deal_date', list_return=True)
        count = 0
        total = len(table_df['table_name'].values)
        now = np.datetime64(datetime.now(),'D')
        df['create_time'] = pandas.to_datetime(df['create_time'], errors='raise')
        df['update_time'] = pandas.to_datetime(df['update_time'], errors='raise')
        temp_df = table_df[((table_df['update_time'].dt.date <= now) | (table_df['create_time'].dt.date <= now))]
        for table_name in temp_df['table_name'].values:
                self.target_db.del_table(table_name)
                count = count + 1
        logger.debug("총 {}개 테이블 중 {}개 삭제 되었습니다..!!".format(total, count))
        print("총 {}개 테이블 중 {}개 삭제 되었습니다..!!".format(total, count))


    def set_universe_real_time(self):
        try:
            if not self.target_db.check_table_exist('universe') :
                return
            sql = "select * from universe"
            df =self.target_db.select_db_to_df(sql)
            fids = get_fid("체결시간")
            self.kiwoom.set_real_reg('8999','',get_fid("장운영구분"),'0')
            range_count = len(df['종목코드'].values)
            screen_num = self.default_screen_num
            count=0

            #if (self.default_end_step - self.end_code_count) < range_count and range_count <= self.default_end_step:
            #    self.default_screen_num = self.default_screen_num + 1

            for str_val in range(0,range_count,self.default_end_step):
                count = count+1
                end_step = self.default_end_step
                if ((range_count)-str_val) < end_step:
                    end_step =(range_count)-str_val
                end_val = str_val+end_step
                codes = df['종목코드'].values[str_val:end_val]
                codes = ";".join(map(str,codes))
                self.kiwoom.set_real_reg(str(screen_num+count),codes,fids,"0")
                #logger.debug("set_universe_real_time(self) : {}".format(codes))
                #logger.debug("실시간 screen  : {}, 코드 갯수  : {} ".format(screen_num+count,end_val-str_val))
                print("실시간 screen  : {}, 코드 갯수  : {} ".format(screen_num + count, end_val - str_val))
                #self.end_code_count = end_val-str_val
                #self.default_screen_num = screen_num

        except Exception as e:
            print(traceback.format_exc())
            send_message(f"현재시간 : {datetime.now()} {e}")
            logger.debug(f"현재시간 : {datetime.now()} {e}")


    def deposit_set(self,insert_bool=False):
        try:
            self.kiwoom.get_order()

            self.kiwoom.get_balance()

            self.base_invest_info['실예수금액'] = self.kiwoom.get_deposit()

            if self.base_invest_info['불용예수금'] == 0 : #불용예수금이 0이면

                if self.base_invest_info['실예수금액'] >= self.base_invest_info['투자가능금'] : #실예수금액이 투자가능금보다 크거나 같을때

                    self.base_invest_info['가용예수금'] = self.base_invest_info['투자가능금']
                    self.base_invest_info['종목당투자'] = self.base_invest_info['가용예수금'] // (self.base_invest_info['최대보유수'] - self.get_balance_count())
                    self.base_invest_info['불용예수금'] = self.base_invest_info['실예수금액'] - self.base_invest_info['투자가능금']
                    insert_bool = True

                else:

                    self.base_invest_info['가용예수금'] = self.base_invest_info['실예수금액']
                    self.base_invest_info['종목당투자'] = self.base_invest_info['가용예수금'] // (self.base_invest_info['최대보유수'] - self.get_balance_count())

            else :

                if self.base_invest_info['실예수금액'] - self.base_invest_info['불용예수금'] >= self.base_invest_info['투자가능금']:

                    self.base_invest_info['가용예수금'] = self.base_invest_info['투자가능금']
                    self.base_invest_info['종목당투자'] = self.base_invest_info['가용예수금'] // (self.base_invest_info['최대보유수'] - self.get_balance_count())
                    self.base_invest_info['불용예수금'] = self.base_invest_info['실예수금액'] - self.base_invest_info['투자가능금']
                    insert_bool = True

                elif self.base_invest_info['실예수금액'] > self.base_invest_info['불용예수금'] :

                    self.base_invest_info['가용예수금'] = self.base_invest_info['실예수금액'] - self.base_invest_info['불용예수금']
                    self.base_invest_info['종목당투자'] = self.base_invest_info['가용예수금'] // (self.base_invest_info['최대보유수'] - self.get_balance_count())

                else:
                    self.base_invest_info['가용예수금'] = 0
                    self.base_invest_info['종목당투자'] = 0

            #print(self.deposit,self.holding_deposit)
            self.base_invest_info['보유종목수'] = self.get_balance_count()
            self.base_invest_info['매수접수수'] = self.get_buy_order_count()
            self.base_invest_info['매도접수수'] = self.get_sell_order_count()

            if insert_bool :

                now = datetime.now().strftime("%Y%m%d")
                temp_df = pandas.DataFrame(data=[[now,'예수금관련','불용예수금', self.base_invest_info['불용예수금']]],columns=['생성일자','작업구분', '값구분', '값'])
                self.target_db.insert_df_to_db("system_base", temp_df, option='upsert',unique_col='작업구분')

        except Exception as e:
            print(traceback.format_exc())

    def exist_deposit(self):
        try:
            if not self.target_db.check_table_exist("system_base"):
                self.deposit_set(True)
            else:

                now = datetime.now().strftime("%Y%m%d")
                sql = "select * from {} where 작업구분 ='예수금관련' and 값구분 = '불용예수금' ORDER BY id DESC LIMIT 1".format('system_base')  # 저장된 데이터의 가장 최근 일자 조회
                temp_df = self.target_db.select_db_to_df(sql)

                if not temp_df.empty:

                    if temp_df['생성일자'].values[0] == now :

                        self.base_invest_info['불용예수금'] = temp_df['값'].values[0]
                        self.deposit_set(False)

                    else:

                        self.base_invest_info['불용예수금'] = temp_df['값'].values[0]
                        self.deposit_set(True)
            return
        except Exception as e:
            print(traceback.format_exc())



    def get_balance_count(self):#매도 주문이 접수되지 않은 보유 종목 수를 계산하는 함수
        try:
            balance_count = len(self.kiwoom.balance)
            #print("get_balance_count")
            #print(balance_count)
            for code in self.kiwoom.order.keys():#kiwoom balance에 존재하는 종목이 매도 주문 접수되었다면 보유 종목에서 제외시킴
                if code in self.kiwoom.balance and '주문구분' in self.kiwoom.order[code].keys() and '미체결수량' in self.kiwoom.order[code].keys():
                    if self.kiwoom.order[code]['주문구분'] == "매도" and self.kiwoom.order[code]['미체결수량'] == 0:
                        balance_count=balance_count-1
            return balance_count
        except Exception as e:
            print(traceback.format_exc())

    def get_buy_order_count(self):#매수 주문 종목 수를 계산하는 함수
        try:
            buy_order_count = 0
            for code in self.kiwoom.order.keys():
                if '주문구분' in self.kiwoom.order[code].keys() and '미체결수량' in self.kiwoom.order[code].keys():
                    if self.kiwoom.order[code]['주문구분'] == '매수' and self.kiwoom.order[code]['미체결수량'] > 0:
                        buy_order_count = buy_order_count+1
            return buy_order_count
        except Exception as e:
            print(traceback.format_exc())

    def get_sell_order_count(self):#매도 주문 종목 수를 계산하는 함수
        try:
            sell_order_count = 0
            for code in self.kiwoom.order.keys():
                if '주문구분' in self.kiwoom.order[code].keys() and '미체결수량' in self.kiwoom.order[code].keys():
                    if self.kiwoom.order[code]['주문구분'] == '매도' and self.kiwoom.order[code]['미체결수량'] > 0:
                        sell_order_count = sell_order_count+1
            return sell_order_count
        except Exception as e:
            print(traceback.format_exc())

    def buy_worker(self,price_type):
        if self.target_db.check_table_exist(table_name='buy_list'):
            sql = "select * from buy_list where 진행상황 = '포착'"
            buy_df = self.target_db.select_db_to_df(sql)
            if not buy_df.empty and '종목코드' in buy_df.columns:
                for index in buy_df.index.values:
                    upsert_buy_df = buy_df.loc[index,:].copy()
                    code = upsert_buy_df['종목코드'].values[0]
                    first_buy = upsert_buy_df['최우선매수호가'].values[0]
                    upsert_buy_df['진행상황'].values[0] = "매수완료"
                    send_message(f"{datetime.now()} 종목코드 : {code} 매수 접수가 시작되었습니다!!.")
                    result_dict = self.check_buy_signal_and_order({'code': code,'price_type': price_type ,'최우선매수호가': first_buy})
                    if result_dict['work'] == '매수' and result_dict['bool']:
                        self.target_db.insert_df_to_db('buy_list', upsert_buy_df, option='upsert', unique_col='등록시간')


    def check_buy_signal_and_order(self,buy_signal_dict):
        try:

            if type(buy_signal_dict) != dict:
                return {'bool': False, 'work': '매수'}

            if not 'code' in buy_signal_dict.keys() :
                return {'bool': False, 'work': '매수'}

            if not 'price_type' in buy_signal_dict.keys() :
                return {'bool': False, 'work': '매수'}

            if not '최우선매수호가' in buy_signal_dict.keys() :
                return {'bool': False, 'work': '매수'}

            print("buy_signal_dict key값 확인 완료")

            if buy_signal_dict['code'] == '':
                return {'bool': False, 'work': '매수'}

            code = buy_signal_dict['code']
            price_type = buy_signal_dict['price_type']
            bid = buy_signal_dict['최우선매수호가']



            #print(self.main.kiwoom.balance.keys())
            if not code in self.kiwoom.balance.keys():

                print(f"{datetime.now()} 종목코드 : {code} 보유내역 없음 완료.")

                self.deposit_set()

                print(f"{datetime.now()} 종목코드 : {code} deposit_set 완료.")
                # 보유종목, 매수 주문 접수한 종목의 합이 보유 가능 최대치라면 더이상 매수 불가능하므로 종료.
                print(self.base_invest_info['보유종목수'] + self.base_invest_info['매수접수수'],self.base_invest_info['최대보유수'])
                if (self.base_invest_info['보유종목수'] + self.base_invest_info['매수접수수'] ) >= self.base_invest_info['최대보유수']:
                    return {'bool': False, 'work': '매수'}

                # 주문에 사용할 금액 계산( self.holding_qut 은 최대 보유 종목수)
                budget = self.base_invest_info['가용예수금']/(self.base_invest_info['최대보유수'] - (self.base_invest_info['보유종목수']+ self.base_invest_info['매수접수수']))

                if self.kiwoom.universe_realtime_transaction_info == {}:
                    return {'bool': False, 'work': '매수'}

                #현재 최우선 매수 호가 확인
                #if code in self.kiwoom.universe_realtime_transaction_info.keys() :
                #    bid = self.kiwoom.universe_realtime_transaction_info[code]['최우선매수호가']
                #else:
                #    bid = buy_signal_dict['target_df']['최우선매수호가'].values[0]

                #주문 수량 계산(소수점은 제거하기 위해 버림)
                quantity = math.floor(budget/bid)

                #주문 주식 수량이 1 미만이라면 매수 불가능 하므로 체크
                if quantity < 1 :
                    return {'bool': False, 'work': '매수'}

                #현재 예수금에서 수수료를 곱한 실제 투입 금액(수량 * 주문가격)을 제외해서 계산
                amount = quantity * bid
                deposit = math.floor(self.base_invest_info['가용예수금'] - amount * (1 + (self.cost_info['수수료률'] *2) + self.cost_info['매도세금'] ))

                #예수금이 0보다 작아질 정도로 주문 할 수는 없으므로 체크
                if deposit < 0:
                    return {'bool': False, 'work': '매수'}

                #계산을 바탕으로 지정가 매수 주문 접수
                order_result = 1
                if price_type == '지정가':
                    order_result = self.kiwoom.send_order('send_buy_order', '1001', 1, code, quantity, bid, '00')
                elif price_type == '시장가':
                    order_result = self.kiwoom.send_order('send_buy_order', '1001', 1, code, quantity, 0, '03')

                if order_result == 0 or order_result == 1:
                    self.base_invest_info['종목당투자'] = budget
                    self.base_invest_info['가용예수금'] = deposit
                    # on_chejan_slot가 늦게 동작할 수도 있기 때문에 미리 약각의 정보를 입력하여 계산에 오류를 방지함.
                    self.kiwoom.order[code] = {'주문구분': '매수', '미체결수량': quantity}
                    send_message(f"{datetime.now()} 종목코드 : {code} 수량 :{quantity} 매수단가 : {bid} .")
                    self.deposit_set()
                    return {'bool': True, 'work': '매수'}
                elif order_result == -308:
                    return {'bool': False, 'work': '매수'}
                else:
                    return {'bool': False, 'work': '매수'}
            else:
                return {'bool': False, 'work': '매수'}
        except Exception as e:
            send_message(f"check_buy_signal_and_order 오류입니다.{traceback.format_exc()}")
            logger.debug(traceback.format_exc())
            print(traceback.format_exc())

    def all_sell_worker(self):
        try:
            count = 0
            regiser_count = self.base_invest_info['보유종목수']
            if regiser_count != 0:
                for target_code in self.kiwoom.balance.keys():
                    if '매매가능수량' in self.kiwoom.balance[target_code].keys():
                        if self.kiwoom.balance[target_code]['매매가능수량'] > 0:
                            amou = self.kiwoom.balance[target_code]['매입가']
                            sell_price = round(amou * (1 + self.base_invest_info['목표수익률']))
                            self.order_sell(target_code, sell_price)
                            print(f"매도 주문 종목명 : {self.kiwoom.balance[target_code]['종목명']}")
                            count = count + 1
                            logger.debug("매도 주문  총 {}개 보유종목중  {} 개 접수완료..!! ".format(regiser_count, count))
                            print("매도 주문  총 {}개 보유종목중  {} 개 접수완료..!! ".format(regiser_count, count))

        except Exception as e:
            send_message(f"order_sell 함수오류입니다.{traceback.format_exc()}")
            logger.debug(traceback.format_exc())
            print(traceback.format_exc())

    def order_sell(self, code, sell_price=0):  # 매도 주문 접수 함수
        try:

            quantity = self.kiwoom.balance[code]['보유수량']  # 보유 수량 확인(전량 매도 방식으로 보유한 수량을 모두 매도함.

            if sell_price != 0:
                # print(1)
                if code in self.kiwoom.universe_realtime_transaction_info.keys():
                    # print(2)
                    now_price = self.kiwoom.universe_realtime_transaction_info[code]['현재가']
                    make_gubun = self.kiwoom.universe_realtime_transaction_info[code]['장구분']
                else:
                    # print(3)
                    now_price = self.kiwoom.balance[code]['현재가']
                    make_gubun = 1
                kosdaq = 2
                # print(4)
                if now_price < 1000:
                    ask = sell_price
                    # print(5)
                elif 1000 <= now_price < 10000:
                    # print(6)
                    ask = round(sell_price, -1)
                elif 10000 <= now_price < 100000 or make_gubun == kosdaq:
                    # print(7)
                    ask = round(sell_price, -2)
                elif 100000 <= now_price:
                    # print(8)
                    ask = round(sell_price, -3)
            else:
                if code in self.kiwoom.universe_realtime_transaction_info.keys():
                    ask = self.kiwoom.universe_realtime_transaction_info[code]['최우선매도호가']  # 최우선 매도 호가 확인
                else:
                    if self.kiwoom.balance[code]['매입가'] * (1 + self.base_invest_info['목표수익률']) > self.kiwoom.balance[code]['현재가']:
                        ask = self.kiwoom.balance[code]['매입가'] * (1 + self.base_invest_info['목표수익률'])
                    else:
                        ask = self.kiwoom.balance[code]['현재가']
            print(f"코드번호 : {code}  수량 : {quantity} 단가 : {ask} ")

            order_result = self.kiwoom.send_order('send_sell_order', '1001', 2, code, quantity, ask, '00')  # kiwoom api를 이용하여 주문접수.

            if order_result == 0 or order_result == 1:
                self.order_bool.update({'bool': True, 'name': '매도'})
                send_message(f"종목코드 : {code} 매도 접수 완료 !!\n {self.kiwoom.balance[code]}")
                logger.debug(f"종목코드 : {code} 매도 접수 완료 !!\n {self.kiwoom.balance[code]}")
                self.deposit_set()
            else:
                self.order_bool.update({'bool': False, 'name': '매도'})
                send_message(f"종목코드 : {code} 매도 접수 실패 !!\n {self.kiwoom.balance[code]}")
                logger.debug(f"종목코드 : {code} 매도 접수 실패 !!\n {self.kiwoom.balance[code]}")

        except Exception as e:
            send_message(f"order_sell 함수오류입니다.{traceback.format_exc()}")
            logger.debug(traceback.format_exc())
            print(traceback.format_exc())

    def run(self):
        try:
            message_time = datetime.now()
            first_start_bool = True
            restart_bool = False
            start_date = datetime.now().strftime('%Y%m%d')
            while self.is_init_succed :

                if self.working and self.trading_day:
                        now = datetime.now()
                        connect_s = self.kiwoom.get_connect_state()

                        if 80001 >= int(datetime.now().strftime('%H%M%S')) >= 80000 :
                            restart_bool =True
                            send_message(f"{now} 오전 8시 재실행 타임입니다.")
                            print(f"{now} 오전 8시 재실행 타임입니다.")

                        if connect_s == 0 or restart_bool :
                            os.system('@taskkill /f /im "python.exe"')

                        if start_date != datetime.now().strftime('%Y%m%d'):
                            self.init_main()
                            send_message(f"{now} 날짜가 바뀌어 초기화 실행합니다.")
                            print(f"{now} 날짜가 바뀌어 초기화 실행합니다.")

                        start_temp = time.time()
                        if self.trading_day and check_transaction_open() :

                            self.real_data_conn['working'] = True
                            self.analysis_data_conn['working'] = True

                            self.base_invest_info_set()

                            self.real_data_conn['real_dict'] = self.kiwoom.universe_realtime_transaction_info.copy()

                            self.all_sell_worker()

                            self.buy_worker('시장가')


                        end_temp = time.time()

                        mess = ""
                        if datetime.now() >= message_time:

                            message_time=datetime.now()+timedelta(minutes=1)
                            for key in self.base_invest_info.keys():
                                mess = mess + f"{key} : {self.base_invest_info[key]}\n"
                            send_message(f"{now}\n 처리시간 : {end_temp-start_temp}\n"+mess)
                            print(f"{now}\n 처리시간 : {end_temp-start_temp}\n"+mess)

        except KeyboardInterrupt :
            print('호출확인')
            self.is_init_succed = False

            self.analy_th.run_bool = False
            self.rD_th.run_bool = False

            self.analy_th.join()
            self.rD_th.join()
        except EOFError:
            pass

        except Exception as e:
            print(traceback.format_exc())
