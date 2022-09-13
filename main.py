import sys
from multiprocessing.spawn import freeze_support

from PyQt5.QtWidgets import QApplication
from execution.execution_main import execution_main


class Main():
    def __init__(self):
        self.app = QApplication(sys.argv)
        self.exe_main = execution_main()
        self.exe_main.start()
        sys.exit(self.app.exec_())

    def __del__(self):
        self.exe_main.is_init_succed=False
        self.exe_main.terminate()

if __name__ == '__main__':
    freeze_support()
    Main()



