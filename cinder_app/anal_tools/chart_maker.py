import traceback

import pandas
from PyQt5.QtCore import QThread

from util.database.db_connecter_0906 import db_connecter


class chart_maker(QThread):
    def __init__(self,parent=None):
        super().__init__()
        self.main = parent
        self.working = True
        self.result_df =pandas.DataFrame()
        self.table_name = ""

    def run(self):
        try:
            print("chart_view")
            select_name = self.table_name
            if select_name != "":
                sql = f"select * from {select_name}"
                db_out = db_connecter(db_info_gubu='local', db_name="cinder")
                df = db_out.select_db_to_df(sql)
                col_list = ['최우선매수']
                mage_col = ['체결시간']
                for col_na in df.columns:
                    for coll in col_list:
                        if col_na.find(coll) != -1 and col_na not in mage_col:
                            #print(col_na, col_na.find(coll), coll)
                            mage_col.append(col_na)
                #print(mage_col[0])
                #print(mage_col[1])
                #print(mage_col[2])
                #print(mage_col[3])
                plan = []
                #plan.append("")#eval(f"((df['고가'] - df['최우선매수호가']) / df['고가']) <= 0.005"))
                plan.append(eval(f"(df['{mage_col[1]}'] >= df['{mage_col[2]}'])"))
                plan.append(eval(f"((df['{mage_col[2]}'] < df['{mage_col[3]}']) & (df['{mage_col[3]}'] < df['{mage_col[4]}']))"))
                #plan.append(eval("((df.저가 * 1.025) >= df.최우선매수호가)"))
                #plan.append("")#eval("(df.거래량RSI14 >= 50)"))
                plan.append(eval("(df.체결강도RSI14 >= 50)"))
                #plan.append(eval("(df.체결강도 >= 70)"))
                comp = []
                for num in range(0,len(plan)):
                    if type(plan[num]) != str :
                        comp.append(f"plan[{num}]")
                codes = " & ".join(map(str, comp))
                result_df = df[eval(eval("codes"))].copy()

                result_df['매수지점'] = result_df['최우선매수호가']
                result_df['목표금액'] = result_df['최우선매수호가'] * 1.025
                mage_col.append('매수지점')
                df = pandas.merge(df.copy(), result_df.copy(), how='outer')
                df=df.replace(0,None)
                mage_dfa = df[mage_col].copy()
                mage_dfa['체결시간'] = mage_dfa['체결시간'].astype("int")
                mage_dfa.set_index('체결시간', inplace=True)

                self.result_df = mage_dfa
                self.working = False
                #print(f"char_maker : {self.working}")
        except Exception as e:
            print(traceback.format_exc())


if __name__ == '__main__':
    try:
        th = chart_maker()
        th.table_name = "analy317830"
        th.start()
    except Exception as e:
        print(traceback.format_exc())
