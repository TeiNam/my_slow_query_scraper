"""Common logging configuration module"""
import logging
import sys

def setup_logger():
    """Configure common logging settings"""
    # 기존 핸들러 모두 제거
    root = logging.getLogger()
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    # 스트림 핸들러 생성 및 설정
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
    stream_handler.setFormatter(formatter)

    # 루트 로거 설정
    root.setLevel(logging.INFO)
    root.addHandler(stream_handler)

    # collectors 네임스페이스 로거 설정
    collectors_logger = logging.getLogger('collectors')
    collectors_logger.setLevel(logging.INFO)

    # my_process_scraper 로거 설정
    scraper_logger = logging.getLogger('collectors.my_process_scraper')
    scraper_logger.setLevel(logging.INFO)

    # uvicorn 액세스 로그 비활성화
    logging.getLogger("uvicorn.access").disabled = True