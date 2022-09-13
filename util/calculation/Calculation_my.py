import numpy as np
import pandas
from imp_collection.base_import import *

class Calculation_my():
    def __init__(self):
        pass

    def rsi_db_to_df(self,df,target_col,period,min_per=1,ris_name='',index_col="",sort_sub="",option=""):

        if ris_name != "":

            ris_name = ris_name

        else:

            ris_name = target_col+ "RIS" + str(period)

        if min_per == 0:

            min_per = period

        if len(df.values) < period :

            period = len(df.values)
            min_per = len(df.values)

        if sort_sub != "" and index_col != "":

            df.sort_values(by=[sort_sub,index_col], ascending=True,inplace=True)

        else:

            df.sort_index(ascending=True,inplace=True)

        if index_col != "" :

            df.set_index(index_col, inplace=True)

        date_index = df.index.astype('str')

        U = np.where(df[target_col].diff(1) > 0, df[target_col].diff(1),0)  # diff함수는 DataFrame함수로 현재행(열)과 바로전행(열)에 차이를 구하는 함수.
        D = np.where(df[target_col].diff(1) < 0, df[target_col].diff(1) * -1, 0)
        AU = pandas.DataFrame(U, index=date_index).rolling(window=period,min_periods=min_per).mean()  # rolling함수는 DataFrame함수로 window에 지정한 행(열)만큼 해당하는 전값들을 계산함수를 이용하여 계산.
        AD = pandas.DataFrame(D, index=date_index).rolling(window=period,min_periods=min_per).mean()
        RSI = AU / (AU + AD) * 100  # RSI(N) 계산, 0부터 1로 표현되는 RSI에 100을 곱함.
        df[ris_name] = RSI
        df = df.fillna(0)

        if option == "":

            if index_col != "" :
                df = df.reset_index()
                df.rename(columns={'index': index_col}, inplace=True)
            return df

        else:

            recent_df = df.iloc[[(len(df.index.values)-1)]]

            return recent_df


    def granville_theory_buy(self,df,index_col,target_col,compare_col="",period=120,duration=10,min_per=1,index_sub="",option=""):

        pass_success = False
        mean_colum_name = target_col+'_이평_' + str(period)

        if not index_sub in df.columns or index_sub == "":

            index_sub = index_col

        if not index_col in df.columns or not target_col in df.columns :

            return {"condition": "","bool":pass_success }

        if not compare_col in df.columns and compare_col != "" :

            return {"condition": "","bool":pass_success }


        if  len(df.values) < period: # data가 이동평균 요청데이터보다 작은지 확인하여 작으면 종료

            return {"condition": "","bool":pass_success }

        else:

            df.sort_values(by=[index_sub, index_col], ascending=True, inplace=True)

            df.set_index(index_col, inplace=True)
            date_index = df.index.astype('str')
            df[target_col] = df[target_col].astype("float")
            ma_df=pandas.DataFrame(df[target_col], index=date_index).rolling(window=period,min_periods=min_per).mean()
            df[mean_colum_name] = ma_df
            df['기준-이평'] = df[target_col] - df[mean_colum_name]

            recent_df = df.iloc[[(len(df.index.values)-1)]]
            recent_df[target_col] = recent_df[target_col].astype("float")
            target_col_recnt_rsi = self.rsi_db_to_df(df.copy(), target_col, period, sort_sub=index_sub,ris_name=target_col+'포지션', option="최근값")[target_col+'포지션'].values[0]
            mean_recnt_rsi = self.rsi_db_to_df(df.copy(), mean_colum_name, period, sort_sub=index_sub,ris_name=mean_colum_name + '포지션', option="최근값")[mean_colum_name + '포지션'].values[0]

            logger.debug(df)
            logger.debug(target_col_recnt_rsi)
            logger.debug(mean_recnt_rsi)
            com_dict = {"매수1신호" : {"조건1" : "target_col_recnt_rsi > 50 and mean_recnt_rsi <= 50",
                                   "조건2" : "recent_df[mean_colum_name].values[0] <= recent_df[target_col].values[0]",
                                   "조건3" : "df.iloc[index_compare_row,col_index] < 0",
                                   "조건4" : "count < duration",
                                   "조건5" : "df.iloc[start_row-1,col_index] > df.iloc[start_row,col_index]"},
                        "매수2신호": {"조건1": "target_col_recnt_rsi < 50 and mean_recnt_rsi >= 50",
                                  "조건2": "recent_df[mean_colum_name].values[0] >= recent_df[target_col].values[0]",
                                  "조건3": "df.iloc[index_compare_row,col_index] > 0",
                                  "조건4": "count < duration",
                                  "조건5": "False"},
                        "매수3신호": {"조건1": "target_col_recnt_rsi >= 50 and mean_recnt_rsi >= 50",
                                  "조건2": "recent_df[mean_colum_name].values[0] < recent_df[target_col].values[0]",
                                  "조건3": "df.iloc[index_compare_row,col_index] > 0",
                                  "조건4": "count < duration",
                                  "조건5": "False"},
                        "매수4신호": {"조건1": "target_col_recnt_rsi >= 50 and mean_recnt_rsi >= 50",
                                  "조건2": "recent_df[mean_colum_name].values[0] > recent_df[target_col].values[0]",
                                  "조건3": "df.iloc[index_compare_row,col_index] < 0",
                                  "조건4": "count < duration",
                                  "조건5": "False"}
                        }
            pass_condition = ""
            for sain_key in com_dict.keys():

                a=eval("com_dict[sain_key]['조건1']")
                if eval(a) : #매수신호 조건1

                    b=eval("com_dict[sain_key]['조건2']")
                    logger.debug("조건1" + sain_key)
                    if eval(b): #매수신호 조건2
                        logger.debug("조건2" + sain_key)
                        dura_bool = True
                        count =0

                        start_index_val = recent_df.index[0]
                        col_index = list(df.columns).index('기준-이평')
                        start_row =list(df.index.values).index(start_index_val)-1
                        index_compare_row = start_row

                        while dura_bool :

                            c=eval("com_dict[sain_key]['조건3']")
                            if eval(c) : #매수신호 조건3

                                index_compare_row=index_compare_row-1
                                count=count+1

                            else:

                                dura_bool =False

                        d=eval("com_dict[sain_key]['조건4']")
                        if eval(d) : #매수신호 조건4

                            pass_condition = sain_key
                            pass_success= False
                            break

                        if compare_col != "":

                            col_index = list(df.columns).index('기준-이평')
                            e=eval("com_dict[sain_key]['조건5']")
                            if eval(e): #매수신호 조건5
                                logger.debug("조건5" + sain_key)
                                pass_condition = sain_key
                                pass_success = False
                                break
                        logger.debug("모두만족" + sain_key)
                        pass_condition = sain_key
                        pass_success = True

            if option == "" and pass_success :

                return {"condition":pass_condition,"bool":pass_success }

            elif option != "" and pass_success :

                df = df.reset_index()
                df.rename(columns={'index': index_col }, inplace=True)
                return df

            else:
                return {"condition": "","bool":pass_success }












