import os
import datetime
import random

_loggers = {}

def get_logger(name):

    return _loggers[name]

class Logger:

    levels = ["ERROR","WARNING","INFO","DEBUG","INTERNAL"]

    def __init__(self,name,dirpath,enabled=True,print_enabled=True,print_level="INFO",datetime_format="%a %b %d %Y %H:%M:%S"):

        self.enabled = enabled

        if enabled:

            self.name = name
            self.log_file_path = os.path.join(dirpath,f"{name}-{datetime.datetime.now().strftime('%d%m%Y-%H%M%S')}-{random.randint(0,99999)}.log")

            self.datetime_format = datetime_format

            self.print_level = self.levels.index(print_level) if type(print_level) is str else print_level
            self.print_enabled = print_enabled

            os.makedirs(dirpath,exist_ok=True)

            self.closed = False
            self._log_file = open(self.log_file_path,"a")

            _loggers[name] = self

            self.log("Logger started.","INTERNAL")

    def log(self,content,level="INFO"):

        if not self.enabled:
            return

        if self.closed:
            raise LoggerClosedError("Can't log with a closed logger.")

        _log_val = self.levels.index(level)

        _c = f"{self.name} | {datetime.datetime.now().strftime(self.datetime_format)} | {level} | {content}"

        self._log_file.write(_c+"\n")
        self._log_file.flush()

        if self.print_enabled and _log_val <= self.print_level:

            print(_c)

    def close(self):

        if not self.enabled:
            return


        if self.closed:

            return

        self.log("Logger closing...","INTERNAL")

        _loggers.pop(self.name)
        self.closed = True
        self._log_file.close()

class LoggerClosedError(Exception):
    pass
