
import os
import hashlib

from . import MAGIC_NUM,VERSION
from . import logging_utils
from . import math_utils
from . import config
from . import structure

DB_KEYVALUE = 0
DB_LIST = 1

OCCUPANCE_NOT_OCCUPIED = b"\x00"
OCCUPANCE_OCCUPIED = b"\x01"
OCCUPANCE_NOT_OCCUPIED_COLLIDED = b"\x02"
OCCUPANCE_OCCUPIED_COLLIDED = b"\x03"

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
            self.keysize_bytesize = math_utils.bytes_needed_to_store_num(self.keysize)
            self.indexsize =  int.from_bytes(db_f.read(1),'little') # default: 12

            struc_size = int.from_bytes(db_f.read(4),'little')
            struc_bytes  = db_f.read(struc_size)
            self.structure = structure.load_structure(struc_bytes.decode('utf-8'))

            self._slotsize = 1 + self.indexsize # occupance info (1B) + index

            conf_size = int.from_bytes(dbf.read(2),'little')
            self.config_location = db_f.tell()
            conf_bytes = conf_size.read(conf_size)
            self.config = config.load(conf_bytes)

            self._slots = int.from_bytes(db_f.read(12),'little')

            self.indices_location = db_f.tell()

            db_f.seek(self._slots*self._slotsize,IO_WHENCE_RELATIVE)

            self.data_location = db_f.tell()

        self.entry_size = 1+self.indexsize+self.keysize_bytesize+self.keysize+len(self.structure)

        self.logger = logging_utils.Logger(self.name, self.config['logger_directory'], self.config["logger_enabled"], self.config["logger_print_enabled"], self.config["logger_print_level"])
        self.log = self.logger.log

        self.log(f"Database ' {self.name} ' initialised with version ' {self.version} ' ,  structure of size {len(self.structure)} ( {len(self.structure/1024/1024)} mb ), at most {int(2**(self.indexsize*8)/self.entry_size)} entries possible.")

    def get_slot(self,key):

        return int.from_bytes(hashlib.sha1(key.encode('ascii')).digest(),'little') % self._slots

    def get_slot_index(self,key):

        return self.indices_location + get_slot(key) * self._slotsize

class Accessor:

    def __init__(self,database):

        self.db = database

        self.db.log("Accessor created.")

        self._file =  open(self.db.location,"rb+")

    def _write_data_at(self,index,key,data,collider=None):

        self._file.seek(self.db.data_location+index)

        self._file.write(OCCUPANCE_OCCUPIED if not collided else OCCUPANCE_OCCUPIED_COLLIDED)
        self._file.write(collider.to_bytes(self.db.indexsize,'little') if collider else b"\x00"*self.db.indexsize)
        self._file.write(len(key).to_bytes(self.db.keysize_bytesize,'little'))
        self._file.write(key.encode("ascii")+(self.db.keysize-len(key))*b"\x00")
        self._file.write(data)

    def set(self,key,data):

        comp_data = self.db.structure.compile(data)

        slot_index = self.get_slot_index(key)

        self._file.seek(slot_index)
        occupance_info = self._file.read(1)
        self._file.seek(slot_index)

        if occupance_info == OCCUPANCE_NOT_OCCUPIED:

            self._file.write(b"\x01")

            dataindex = int(os.path.getsize(self.db.location))-self.db.data_location
            self._file.write(dataindex.to_bytes(self.db.indexsize,"little"))

            self._write_data_at(dataindex,key,comp_data)

        elif occupance_info == OCCUPANCE_OCCUPIED:

            self._file.seek(1,1)

            o_dataindex = self._file.read(self.db.indexsize)

            self._file.seek(self.db.data_location+o_dataindex)

            data_occupance_info =  self._file.read(1)

            if data_occupance_info == OCCUPANCE_NOT_OCCUPIED:

                self._write_data_at(o_dataindex,key,comp_data)

            elif data_occupance_info in [OCCUPANCE_OCCUPIED_COLLIDED,OCCUPANCE_NOT_OCCUPIED_COLLIDED]:

                cur_dataindex = o_dataindex

                potential_index = None
                potential_index_original_collider = None

                while True:

                    self._file.seek(self.db.data_location+cur_dataindex)

                    data_occupance_info =  self._file.read(1)

                    collided_index = int.from_bytes(self._file.read(self.db.indexsize),'little')

                    key_r_len = int.from_bytes(self._file.read(self.db.keysize_bytesize),'little')
                    key_r = self._file.read(self.db.keysize[:key_r])

                    if key_r = key:

                        self._write_data_at(cur_dataindex,key,comp_data)
                        break

                    else:

                        if data_occupance_info == OCCUPANCE_OCCUPIED_COLLIDED:

                            cur_dataindex = collided_index

                        elif data_occupance_info == OCCUPANCE_NOT_OCCUPIED:

                            self._write_data_at(cur_dataindex,key,comp_data)
                            break

                        elif data_occupance_info == OCCUPANCE_OCCUPIED:

                            new_dataindex = int(os.path.getsize(self.db.location))-self.db.data_location if not potential_index else potential_index

                            self._file.seek(self.db.data_location+cur_dataindex)
                            sel._file.write(OCCUPANCE_OCCUPIED_COLLIDED)
                            self._file.write(new_dataindex.to_bytes(self.db.indexsize,"little"))

                            self._write_data_at(new_dataindex,key,comp_data,None if not potential_index else potential_index_original_collider)
                            break

                        elif data_occupance_info == OCCUPANCE_NOT_OCCUPIED_COLLIDED

                            potential_index = cur_dataindex
                            potential_index_original_collider = collided_index

                            if collided_index == o_dataindex: # To evade infinite looping

                                self._write_data_at(cur_dataindex,key,comp_data,collided_index)
                                break

                            cur_dataindex = collided_index

    def get(self,key):

        slot_index = self.get_slot_index(key)

        self._file.seek(slot_index)
        occupance_info = self._file.read(1)
        self._file.seek(slot_index)

        if occupance_info == OCCUPANCE_NOT_OCCUPIED:

            return None

        elif occupance_info == OCCUPANCE_OCCUPIED:

            self._file.seek(1,1)

            o_dataindex = self._file.read(self.db.indexsize)

            self._file.seek(self.db.data_location+o_dataindex)

            data_occupance_info =  self._file.read(1)

            if data_occupance_info == OCCUPANCE_NOT_OCCUPIED:

                return None

            elif data_occupance_info in [OCCUPANCE_OCCUPIED_COLLIDED,OCCUPANCE_NOT_OCCUPIED_COLLIDED]:

                cur_dataindex = o_dataindex

                while True:

                    self._file.seek(self.db.data_location+cur_dataindex)

                    data_occupance_info =  self._file.read(1)

                    collided_index = int.from_bytes(self._file.read(self.db.indexsize),'little')

                    key_r_len = int.from_bytes(self._file.read(self.db.keysize_bytesize),'little')
                    key_r = self._file.read(self.db.keysize[:key_r])

                    if key_r = key:

                        if data_occupance_info in [OCCUPANCE_OCCUPIED,OCCUPANCE_OCCUPIED_COLLIDED]:

                            return self.db.structure.fetch(self._file.read(len(self.db.structure)))

                        else:

                            return None

                    else:

                        if data_occupance_info == OCCUPANCE_OCCUPIED_COLLIDED:

                            cur_dataindex = collided_index

                        elif data_occupance_info == OCCUPANCE_NOT_OCCUPIED:

                            return None

                        elif data_occupance_info == OCCUPANCE_OCCUPIED:

                            return None

                        elif data_occupance_info == OCCUPANCE_NOT_OCCUPIED_COLLIDED

                            if collided_index == o_dataindex: # To evade infinite looping

                                return None

                            cur_dataindex = collided_index

class WrongMagicNum(Exception):

    pass
