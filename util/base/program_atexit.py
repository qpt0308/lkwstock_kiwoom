from datetime import datetime
import atexit
from util.base.notifier import *

@atexit.register
def goodbye():
    print(f"{datetime.now()} 프로그램이 종료되었습니다.")
    send_message(f"{datetime.now()} 프로그램이 종료되었습니다.")

