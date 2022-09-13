import multiprocessing
import os
import platform
import socket
import sys
import time
import traceback
from datetime import datetime
from multiprocessing import freeze_support

import matplotlib
import numpy as np
import plotly.io
from PyQt5.QAxContainer import *
from PyQt5.QtCore import *
from PyQt5.QtTest import QTest
from matplotlib import pyplot as plt
from PyQt5.QtWidgets import *

from cinder_app.anal_tools.chart_maker import chart_maker
from cinder_app.anal_tools.db_sever_to_local import db_sever_to_local
from util.database.db_connecter_0906 import db_connecter

freeze_support()

class MyMainGUI(QMainWindow):

        def __init__(self, parent=None):
                super().__init__(parent)

                self.btn1 = QPushButton("DB in_out data loding",self)
                self.btn2 = QPushButton("Cinder DB loding",self)

                self.count_label = QLabel("    ", self)
                self.count_label.resize(100, 20)

                self.listwidget = QListWidget(self)
                self.listwidget.resize(100, 550)

                vbox = QVBoxLayout()
                vbox.addWidget(self.btn1)
                vbox.addWidget(self.btn2)
                vbox.addWidget(self.count_label)
                vbox.addWidget(self.listwidget)

                self.setWindowTitle('My_analysis')
                self.setGeometry(100,100,600,600)
                centralWidget = QWidget()
                centralWidget.setLayout(vbox)
                self.setCentralWidget(centralWidget)

        def center(self):
                qr = self.frameGeometry()
                cp = QDesktopWidget().availableGeometry().center()
                qr.moveCenter(cp)
                self.move(qr.topLeft())



class My_analysis(MyMainGUI):
        add_siganl = pyqtSignal()
        send_instance_singql = pyqtSignal("PyQt_PyObject")

        def __init__(self,parent=None):
                super().__init__(parent)
                self.count = 0
                self.total = 0
                self.target_db = db_connecter(db_info_gubu='local', db_name="cinder")

                self.btn1.clicked.connect(self.data_loding)
                self.btn2.clicked.connect(self.cinder_loding)


                self.th1 = db_sever_to_local(parent=self)
                self.th1.list_changed.connect(self.listwiget_data)

                self.th2 = chart_maker(parent=self)

                self.listwidget.itemClicked.connect(self.chart_view)
                self.show()

        @pyqtSlot()
        def data_loding(self):
                self.btn1.setEnabled(False)
                self.th1.working = True
                self.th1.start()
                self.th1.work_gubun = "db_in_out"

        def cinder_loding(self):
                item_dict={}
                item_dict['statu'] = "end"
                self.listwiget_data(item_dict)

        @pyqtSlot()
        def chart_view(self):
                try:
                        self.th2.table_name = self.listwidget.selectedItems()[0].text()
                        self.th2.start()
                        chart_bool = True
                        while chart_bool :
                                #print(self.th2.working)
                                QTest.qWait(1)
                                if self.th2.working == False:
                                        print("성공??")
                                        mage_dfa = self.th2.result_df.copy()
                                        print(mage_dfa[mage_dfa['매수지점'] > 0])
                                        col_list = ['최우선매수', '매수지점']
                                        color = ['red', 'blue', 'yellow', 'black', 'orange']
                                        m_size = [2, 2, 2, 2, 10]
                                        co =0
                                        if platform.system() == 'Windows':
                                                matplotlib.rc('font', family='Malgun Gothic')
                                        elif platform.system() == 'Darwin':
                                                matplotlib.rc('font', family='AppleGothic')
                                        else:
                                                matplotlib.rc('font', family='NanumGothic')
                                        fig = plt.figure(figsize=(30, 5))
                                        ax = fig.add_subplot(1, 1, 1)
                                        for col_name in mage_dfa.columns:
                                                ax.plot(mage_dfa.index, mage_dfa[col_name], marker='o',
                                                        markerfacecolor=color[co], markersize=m_size[co], color=color[co], linewidth=1,
                                                        label=col_name)
                                                co = co + 1
                                        plt.title(self.th2.table_name)
                                        plt.legend()
                                        plt.show()
                                        chart_bool=False
                                        self.th2.working=True
                                        print("실행끝!!")


                except Exception as e:
                        print(traceback.format_exc())

        @pyqtSlot()
        def thead_working_stop(self,th_temp):
                th_temp.working = False

        def listwiget_data(self,item_dict):
                #print(item_dict)
                if item_dict['statu'] == "ing":
                        self.count_label.setText("{}/{}진행중입니다..!!".format(self.count,item_dict['total']))
                        self.count=self.count+1
                elif item_dict['statu'] == "end":
                        db = self.target_db.check_table_exist(table_name='analy',list_return=True)
                        cout =0
                        self.total =len(db.index)
                        for table_name in db['table_name'].values:
                            self.listwidget.insertItem(cout, table_name)
                            self.listwidget.repaint()
                            cout=cout+1
                        self.count=cout
                        self.btn1.setEnabled(True)
                        self.count_label.setText("{}/{}완료했습니다.".format(self.count,self.total))

if __name__ == "__main__":
        app = QApplication(sys.argv)
        widget = My_analysis()
        widget.show()
        sys.exit(app.exec_())