import logging
import datetime


def CreateLogger():
    logger = logging.getLogger(__name__)

    if len(logger.handlers) > 0:
        return logger

    #log 메시지 형식 설정
    format = logging.Formatter("%(asctime)s | %(filename)s | %(lineno)s | %(levelname)s -> %(message)s")
    """ 
    #메시지 형식 설정 저장
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(format)
    logger.addHandler(streamHandler)
"""
    #위 log를 파일로 저장하는 부분
    d_time = datetime.datetime.now()
    d_str = d_time.strftime("%Y-%m-%d")
    fileHandler = logging.FileHandler("log/"+d_str+".log",encoding='utf-8')
    fileHandler.setFormatter(format)
    logger.addHandler(fileHandler)

    logger.setLevel(level=logging.DEBUG)
    return logger

logger = CreateLogger()

