import time
from datetime import datetime, timedelta

import numpy as np
import pandas
import mariadb
from util.database.db_info import *


class db_connecter():
    def __init__(self, db_info_gubu="local", host_name="", port_num="", db_name="", user="", pw="", pool_bool=True):

        self.pl_name = "lkwPool" + datetime.now().strftime('%f')

        # if main != None:
        #    self.pl_name = str(main.__module__).replace(".","")

        self._db_info = {'host': host_name,
                         'port': port_num,
                         'database': db_name,
                         'user': user,
                         'password': pw
                         }

        self.db_info_gubu = db_info_gubu

        self._now_db_info_bool = False

        self._exist_skip_dic = {}

        self.pool_bool = pool_bool

        self._init_fun(db_info_gubu)

    def _init_fun(self, gubu):

        # qubu정보를 이용하여 호출자가 입력한 정보중 없는 정보 입력
        if gubu in db_info_name.keys():
            for key in db_info_name[gubu].keys():
                if self._db_info[key] == "":
                    self._db_info[key] = db_info_name[gubu][key]
        else:
            for key in self._db_info.keys():
                if self._db_info[key] == "":
                    # logger.debug("요청하신 연결에 입력값이 누락되었습니다..!!다시 확인 하세요..")
                    return
            self._now_db_info_bool = True

        if not self._now_db_info_bool:
            if self._db_info['database'] != db_info_name[gubu]['database']:
                self._base_exist(self._db_info['database'])
        else:
            self._base_exist(self._db_info['database'])

        try:
            #print(list(mariadb._POOLS.keys()))
            #if not self.pl_name in list(mariadb._POOLS.keys()):
                #print(f"{self.pl_name} pool 신규셋팅 시작")
                pool = mariadb.ConnectionPool(pool_name=self.pl_name,
                                          host=self._db_info['host'],
                                          user=self._db_info['user'],
                                          password=self._db_info['password'],
                                          database=self._db_info['database'],
                                          pool_size=64,
                                          pool_reset_connection=True)
                self.pool = pool
            #else:
                #print(f"{self.pl_name} pool 기존풀 시작")
            #    self.pool = mariadb.ConnectionPool(pool_name=self.pl_name)
            #print(list(mariadb._POOLS.keys())[0])

        except mariadb.ProgrammingError as me:
            print(f"초기오류 {me}")

    def all_analyze_table(self):
        sql = f"select table_name, table_rows from information_schema.tables where table_schema = '{self._db_info['database']}'"
        all_table_df = self.select_db_to_df(sql)
        for table_name in all_table_df['table_name'].values :
            sql = f"ANALYZE TABLE {table_name}"
            self._conn_fetchone(sql)
            self.all_analyze_bool = True
        #print("all table analyze update 작업완료..!!")

    def one_analyze_table(self,table_name):
        if self.check_table_exist(table_name=table_name):
            sql = f"ANALYZE TABLE {table_name}"
            self._conn_fetchone(sql)
            #print(f'{table_name} table analyze 작업완료..!!')

    def _base_exist(self, db_na):
        exist_db = mariadb.connect(host=self._db_info['host'], user=self._db_info['user'],
                                   password=self._db_info['password'])
        mydbcur = exist_db.cursor()
        mydbcur.execute('SHOW DATABASES')
        if str(mydbcur.fetchall()).find(db_na) == -1:
            mydbcur.execute('CREATE DATABASE {}'.format(db_na))
            exist_db.commit()
        exist_db.close()

    def _create_get_conn(self):
        try:
            #print("pool 연결")
            con = self.pool.get_connection()
            cur = con.cursor()
            return con,cur
        except Exception as e:
            #print("일반연결")
            #print(f"_create_get_conn{e}")
            con = mariadb.connect(**self._db_info)
            cur = con.cursor()
            return con,cur

    def _deault_get_conn(self):
            con = mariadb.connect(**self._db_info)
            return con

    def _conn_fetchone(self, sql):
        try:
            con,cur = self._create_get_conn()
            cur.execute(sql)
        except mariadb.InterfaceError as e:
            con.close()
            con, cur = self._create_get_conn()
            cur.execute(sql)
        con.commit()
        con.close()


    def _conn_fetchall_df(self, sql):
        try:
            con,cur = self._create_get_conn()
            cur.execute(sql)
        except mariadb.InterfaceError as e:
            con.close()
            con, cur = self._create_get_conn()
            cur.execute(sql)
        temp_date = cur.fetchall()
        # print(temp_date)
        temp_name = cur.description
        # print(temp_name)
        con.commit()
        con.close()
        cout = 0
        df = {}
        for x in list(temp_name):
            # col_name.append(list(x)[0])
            col_name = list(x)[0]
            df_data = []
            for y in temp_date:
                df_data.append(list(y)[cout])
            cout = cout + 1
            df.update({col_name: df_data})

        # print(df)

        result = pandas.DataFrame(df)
        # print(result)

        return result

    def _conn_executemany(self, sql, list):
        try:
            con,cur = self._create_get_conn()
            cur.executemany(sql, list)
        except mariadb.InterfaceError as e:
            con.close()
            con, cur = self._create_get_conn()
            cur.executemany(sql, list)
        con.commit()
        con.close()

    def index_exist(self, table_name, unique_col="id"):

        sql = f"SHOW INDEX FROM {table_name} WHERE KEY_NAME = 'uk_name'"
        temp = self.select_db_to_df(sql)

        if not temp.empty:

            if temp['Key_name'].values[0] == 'uk_name':
                sql = f"ALTER TABLE {table_name} DROP INDEX uk_name"
                self._conn_fetchone(sql)

                sql = f"ALTER TABLE {table_name} ADD UNIQUE KEY uk_name({unique_col})"
                self._conn_fetchone(sql)
        else:
            sql = f"ALTER TABLE {table_name} ADD UNIQUE KEY uk_name({unique_col})"
            self._conn_fetchone(sql)
        self.one_analyze_table(table_name=table_name)

    def check_table_exist(self, table_name="", list_return=False):

        if list_return or table_name == "":
            sql = f"select table_name, table_rows, update_time, create_time from information_schema.TABLES where TABLE_SCHEMA = '{self._db_info['database']}' and TABLE_NAME LIKE '{table_name}%';"
        else:
            sql = f"SELECT table_name, table_rows, update_time, create_time FROM Information_schema.tables WHERE table_schema = '{self._db_info['database']}' AND table_name = '{table_name}'"

        result = self._conn_fetchall_df(sql)

        if not result.empty and not list_return and table_name != "":
            return True
        elif list_return:
            return result
        else:
            return False

    def check_colum_exist(self, table_name, col_dict={}, option=''):
        sql = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{self._db_info['database']}' AND TABLE_NAME = '{table_name}';"
        result = self._conn_fetchall_df(sql)
        resulta = []
        if not result.empty:
            resulta = list(result['COLUMN_NAME'].values)
            count = 0
            not_col_dict = {}
            for col_name in col_dict.keys():
                if not col_name in result['COLUMN_NAME'].values:
                    count = count + 1
                    not_col_dict[col_name] = col_dict[col_name]

            if count != 0 and option == 'append':
                col_info = self.col_name_type_mk(not_col_dict)
                sql = "ALTER TABLE {} ADD ({})".format(table_name, col_info)
                self._conn_fetchone(sql)
            else:
                pass
        return resulta

    def col_name_type_mk(self, col_dict):
        col_info_list = []
        for col in col_dict.keys():
            if str(col_dict[col]).find('int') != -1:
                this_type = 'DOUBLE'
            elif str(col_dict[col]).find('str') != -1:
                this_type = 'TEXT'
            elif str(col_dict[col]).find('float') != -1:
                this_type = 'FLOAT(11)'
            elif str(col_dict[col]).find('numpy.datetime64') != -1:
                this_type = 'DATETIME'
            else:
                this_type = 'TEXT'
            col_info_list.append(" {} {}".format(col, this_type))
        col_info = ",".join(map(str, col_info_list))
        # logger.debug("{} name,type 인자열 생성완료..!!".format(col_info))
        return col_info

    def select_db_to_df(self, sql):
        resulte_df = self._conn_fetchall_df(sql)
        return resulte_df

    def create_table(self, col_dict, table_name):
        col_info = self.col_name_type_mk(col_dict)
        sql = "CREATE TABLE {} (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, {} ,UNIQUE KEY uk_name(id))".format(
            table_name, col_info)
        self._conn_fetchone(sql)
        # logger.debug("{} Table 생성완료..!!".format(table_name))

    def del_table(self, table_name, con=""):
        if self.check_table_exist(table_name=table_name):
            sql = "DROP TABLE IF EXISTS {}".format(table_name)
            self._conn_fetchone(sql)
        # logger.debug("{} Table 삭제 완료..!!".format(table_name))

    def insert_df_to_db(self, table_name, df, option="replace", unique_col=""):
        try:
            # logger.debug("insert_df_to_db 시작")
            turn = True  # replace의 경우 테이블 유무에 따라 삭제와 생성 그리고 삽입에 단계로 인한 반복제어문.
            col_check_option = 'append'  # 실제db에 컬럼리스트 요청시 필요사항.
            create_bool = False
            col_count_list = []
            if 'id' in df.columns:
                del df['id']
            for i in df.columns:
                col_count_list.append("%s")
            col_info = ",".join(map(str, col_count_list))

            if not self.check_table_exist(table_name):
                self.create_table(df.dtypes.to_dict(), table_name)
                create_bool = True

            while turn:
                if option == 'append' or create_bool:

                    self.index_exist(table_name)

                    self.check_colum_exist(table_name=table_name,col_dict=df.dtypes.to_dict(),option='append')

                    sql = "INSERT INTO {} VALUES (NULL,{})".format(table_name, col_info)

                    #print(table_name,sql)

                    self._conn_executemany(sql, df.values.tolist())

                    turn = False

                    # logger.debug('{}개 행 입력완료..!!'.format(len(df.values.tolist())))

                elif option == 'replace':
                    if not create_bool:
                        self.del_table(table_name)
                        self.create_table(df.dtypes.to_dict(), table_name)
                        create_bool = True

                elif option == 'upsert':
                    if unique_col != "":

                        self.index_exist(table_name, unique_col=unique_col)

                        col_list = list(df.columns)
                        upsert_list = []

                        for col in df.columns:
                            if col != unique_col:
                                upsert_list.append("{} = VALUES({})".format(col, col))
                        upsert_col_info = ",".join(map(str, col_list))
                        upsert_info = ",".join(map(str, upsert_list))

                        sql = "INSERT INTO {} (id,{}) VALUES (NULL,{}) ON DUPLICATE KEY UPDATE {}".format(table_name,
                                                                                                          upsert_col_info,
                                                                                                          col_info,
                                                                                                          upsert_info)
                        self._conn_executemany(sql, df.values.tolist())
                        turn = False

                    else:
                        turn = False
        except Exception as e:
            print(e)


if __name__ == '__main__':
    # db_out = db_connecter(db_info_gubu='azure')
    db_in = db_connecter(db_info_gubu='local')
    # table_list = db_out.check_table_exist( table_name="real_deal_date", list_return=True)
    # for table_name in table_list['table_name'].values :
    #    print(table_name)
    #    sql = f'select * from {table_name}'
    #    in_df=db_out.select_db_to_df(sql)
    #    in_df = in_df.drop(['id'],axis=1)
    #    db_in.insert_df_to_db(table_name,in_df)

    sql = f"select table_name, table_rows, update_time, create_time from information_schema.tables where table_schema = 'lkwstock'"
    df = db_in.select_db_to_df(sql)
    df_table = db_in.check_table_exist(table_name="real_deal_date", list_return=True)
    df.sort_values(by=['update_time'], ascending=False, inplace=True)
    print(df)
    print(df['create_time'].values[0])
    now = np.datetime64(datetime.now()-timedelta(minutes=20),'s')
    now_time1 = np.datetime64('2022-09-13 06:30:30','D')
    now_time2 = np.datetime64(datetime.now(),'D')
    print(now_time1,now_time2)
    df['create_time'] = pandas.to_datetime(df['create_time'],errors='raise')
    print(df[df['create_time'].dt.date == now_time2])
