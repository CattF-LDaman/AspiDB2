
import os

from . import MAGIC_NUM,VERSION
from . import logging_utils
from . import config

DB_KEYVALUE = 0
DB_LIST = 1

IO_WHENCE_START = 0
IO_WHENCE_RELATIVE = 1
IO_WHENCE_END = 2

class Database:

    def __init__(self,location):

        self.location = location

        if not os.path.isfile(location):

            raise FileNotFoundError(f"Didn't find file at '{location}'")

        with open(location,"rb") as db_f:

            if db_f.read(len(MAGIC_NUM)) != MAGIC_NUM:

                raise WrongMagicNum("Didn't find magic number at beginning of file at location")

            dbn_len = db_f.read(4)
            self.name = db_f.read(int.from_bytes(dbn_len,'little'))

            self.type = int.from_bytes(db_f.read(1),'little')

            self.version = int.from_bytes(db_f.read(3),'little')

            self.keysize = int.from_bytes(db_f.read(2),'little')
            self.indexsize =  int.from_bytes(db_f.read(1),'little') # default: 12

            struc_size = int.from_bytes(db_f.read(4),'little')
            struc_bytes  = db_f.read(struc_size)

            self._slotsize = 1 + self.indexsize # occupance info (1B) + index

            conf_size = int.from_bytes(dbf.read(2),'little')
            self.config_location = db_f.tell()
            conf_bytes = conf_size.read(conf_size)
            self.config = config.load(conf_bytes)

            self._slots = int.from_bytes(db_f.read(12),'little')

            self.indices_location = db_f.tell()

            db_f.seek(self._slots*self._slotsize,IO_WHENCE_RELATIVE)

            self.data_location = db_f.tell()

        self.logger = logging_utils.Logger(self.name, self.config['logger_directory'], self.config["logger_enabled"], self.config["logger_print_enabled"], self.config["logger_print_level"])
        self.log = self.logger.log

        self.log(f"Database ' {self.name} ' initialised with version ' {self.version} ' ,  ")

    def get_slot():

class WrongMagicNum(Exception):

    pass
