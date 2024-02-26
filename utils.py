import logging
import time
import sys
import os

from datetime import datetime
from conf import log_folder

log_file_folder = log_folder+f'\\{datetime.today().strftime("%Y.%m.%d")}'
isExist = os.path.exists(log_file_folder)
if not isExist:
    os.makedirs(log_file_folder)

logging.basicConfig(level=logging.INFO,
                    handlers=[
                        logging.StreamHandler(sys.stdout),
                        logging.FileHandler(log_file_folder+f'\\msp_registry_upload_{datetime.today().strftime("%Y.%m.%d")}.log', mode="w"),
                    ],
                    format='%(asctime)s: %(levelname)s - %(message)s')

def timeit(method):
    def timed(*args, **kwargs):
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()
        if 'log_time' in kwargs:
            name = kwargs.get('log_name', method.__name__.upper())
            kwargs['log_time'][name] = int(te - ts)
        else:
            print('%r  %2.22f ms' % (method.__name__, (te - ts)))
        return result
    return timed

def error_handler(error_type, log_message):
    if error_type == 'I':
        logging.info(log_message)
    elif error_type == 'W':
        logging.warning(log_message)
    else:
        logging.error(log_message)
    print()