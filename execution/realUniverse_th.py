from multiprocessing import Process
from multiprocessing.spawn import freeze_support

from PyQt5.QtCore import QThread, pyqtSignal
from util.data_bring.make_up_universe_A import *
from api.const import *
from imp_collection.base_import import *
from util.database.db_connecter import db_connecter

freeze_support()

class realUniverse_th(Process):

    def __init__(self,process_conn):
        super(realUniverse_th, self).__init__()
        self.process_conn = process_conn
        self.run_bool = True
        self.working = True
        self.base_invest_info = self.process_conn['base_invest_info'].copy()

    def __del__(self):
        send_message(f"{datetime.now()} realUniverse_th 종료되었습니다.!!")
        logger.debug(f"{datetime.now()} realUniverse_th 종료되었습니다.!!")

    def check_and_get_universe(self): # 유니버스가 있는지 확인하고 없으면 생성하는 함수
        try:

            now = datetime.now().strftime("%Y%m%d")# 반환값은 dataframe으로 반환되면 "종목코드","종목명","현재가"로 되어있음. 여기에 오늘날짜에 생성일자추가.
            if not self.target_db.check_table_exist("universe"): #유니버스 테이블이 있는지 체크.(있을때 True 없을때 False)그래서 not을 붙음)
                universe_df = get_universe()  # make_up_universe.py함수 호출
                #print(universe_df)
                universe_df = universe_df[universe_df['현재가'] <= self.base_invest_info['종목당투자']]
                universe_df.insert(0, "생성일자", now)
                self.target_db.insert_df_to_db("universe", universe_df)  # db_helper.py에 insert_df_to_db함수 호출.
            else:
                sql = "select max({}) from {}".format('생성일자', "universe")  # 저장된 데이터의 가장 최근 일자 조회
                cur = self.target_db.select_db_to_df(sql)
                #print(cur)
                before_date = cur['max(생성일자)'].iloc[0]
                if before_date != now :
                    universe_df = get_universe()  # make_up_universe.py함수 호출
                    universe_df = universe_df[universe_df['현재가'] <= self.base_invest_info['종목당투자']]
                    #print(universe_df)
                    universe_df.insert(0, "생성일자", now)
                    self.target_db.insert_df_to_db("universe", universe_df)  # db_helper.py에 insert_df_to_db함수 호출.

        except Exception as e:
            print(traceback.format_exc())
            send_message(f"현재시간 : {datetime.now()} {e}")
            logger.debug(f"현재시간 : {datetime.now()} {e}")



    def run(self) :
        self.target_db = db_connecter(db_info_gubu=self.process_conn['db_info_gubu'])
        self.check_and_get_universe()
        message_time = datetime.now()
        while self.run_bool:
            try:
                if self.working :
                    start_temp = time.time()

                    end_temp = time.time()
                    if datetime.now() >= message_time:
                        message_time = datetime.now() + timedelta(minutes=1)
                        send_message(f"{datetime.now()}realUniverse_th 실행중입니다.\n 처리시간 : {end_temp - start_temp}")
                        logger.debug(f"{datetime.now()}realUniverse_th 실행중입니다.\n 처리시간 : {end_temp - start_temp}")
                        print(f"{datetime.now()} realUniverse_th 실행중입니다. 처리시간 : {end_temp - start_temp}")

            except KeyboardInterrupt:
                self.run_bool = False
                print(f'{self.__class__}KeyboardInterrupt')

