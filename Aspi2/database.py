
import os
import hashlib

from . import MAGIC_NUM,VERSION
from . import logging_utils
from . import math_utils
from . import find_operators
from . import config
from . import structure

def build(location,name,struc,slots=1024,max_key_length=64,index_size_bytes=12):

    with open(location,"wb") as f:

        f.write(MAGIC_NUM)

        f.write(len(name.encode('utf-8')).to_bytes(2,"little"))
        f.write(name.encode('utf-8'))

        f.write(VERSION.to_bytes(3,'little'))

        f.write(max_key_length.to_bytes(2,'little'))
        f.write(index_size_bytes.to_bytes(1,'little'))

        c_struc = structure.compile_structure(struc).encode('utf-8')
        f.write(len(c_struc).to_bytes(4,'little'))
        f.write(c_struc)

        f.write(slots.to_bytes(12,'little'))

        f.seek(slots*(index_size_bytes+1),1)
        f.write(b"\x00")

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

    def __init__(self,location,db_config = {}):

        self.location = location

        if not os.path.isfile(location):

            raise FileNotFoundError(f"Didn't find file at '{location}'")

        with open(location,"rb") as db_f:

            if db_f.read(len(MAGIC_NUM)) != MAGIC_NUM:

                raise WrongMagicNum("Didn't find magic number at beginning of file at location")

            dbn_len = db_f.read(2)
            self.name = db_f.read(int.from_bytes(dbn_len,'little')).decode('utf-8')

            self.version = int.from_bytes(db_f.read(3),'little')

            self.keysize = int.from_bytes(db_f.read(2),'little')
            self.keysize_bytesize = math_utils.bytes_needed_to_store_num(self.keysize)
            self.indexsize =  int.from_bytes(db_f.read(1),'little') # default: 12

            struc_size = int.from_bytes(db_f.read(4),'little')
            struc_bytes  = db_f.read(struc_size)
            self.structure = structure.load_structure(struc_bytes.decode('utf-8'))

            self._slotsize = 1 + self.indexsize # occupance info (1B) + index

            self.config = config.load(db_config)

            self._slots = int.from_bytes(db_f.read(12),'little')

            self.indices_location = db_f.tell()

            db_f.seek(self._slots*self._slotsize,IO_WHENCE_RELATIVE)

            self.data_location = db_f.tell()

        self.entry_size = 1+self.indexsize+self.keysize_bytesize+self.keysize+len(self.structure)

        self.logger = logging_utils.Logger(self.name, self.config['logger_directory'], self.config["logger_enabled"], self.config["logger_print_enabled"], self.config["logger_print_level"])
        self.log = self.logger.log

        self.log(f"Database ' {self.name} ' initialised with version ' {self.version} ' (Client version is ' {VERSION} '),  structure of size {len(self.structure)} ( {len(self.structure)/1024/1024} mb ), at most {int(2**(self.indexsize*8)/self.entry_size)} entries possible.")

    def get_slot(self,key):

        return int.from_bytes(hashlib.sha1(key.encode('ascii')).digest(),'little') % self._slots

    def get_slot_index(self,key):

        return self.indices_location + self.get_slot(key) * self._slotsize

class Accessor:

    def __init__(self,database):

        self.db = database

        self.db.log("Accessor created.")

        self._file =  open(self.db.location,"rb+")

    def _write_data_at(self,index,key,data,collider=None):

        self._file.seek(self.db.data_location+index)

        self._file.write(OCCUPANCE_OCCUPIED if not collider else OCCUPANCE_OCCUPIED_COLLIDED)
        self._file.write(collider.to_bytes(self.db.indexsize,'little') if collider else b"\x00"*self.db.indexsize)
        self._file.write(len(key).to_bytes(self.db.keysize_bytesize,'little'))
        self._file.write(key.encode("ascii")+(self.db.keysize-len(key))*b"\x00")
        self._file.write(data)

    def delete(self,key):

        self.db.log(f"DELETE {key}")

        slot_index = self.db.get_slot_index(key)

        self._file.seek(slot_index)
        occupance_info = self._file.read(1)
        self._file.seek(slot_index)

        if occupance_info == OCCUPANCE_NOT_OCCUPIED:

            return None

        elif occupance_info == OCCUPANCE_OCCUPIED:

            self._file.seek(1,1)

            o_dataindex = int.from_bytes(self._file.read(self.db.indexsize),'little')

            self._file.seek(self.db.data_location+o_dataindex)

            data_occupance_info =  self._file.read(1)

            if data_occupance_info == OCCUPANCE_NOT_OCCUPIED:

                self._file.seek(self.db.data_location+cur_dataindex)

            elif data_occupance_info in [OCCUPANCE_OCCUPIED_COLLIDED,OCCUPANCE_OCCUPIED,OCCUPANCE_NOT_OCCUPIED_COLLIDED]:

                cur_dataindex = o_dataindex

                while True:

                    self._file.seek(self.db.data_location+cur_dataindex)

                    data_occupance_info =  self._file.read(1)

                    collided_index = int.from_bytes(self._file.read(self.db.indexsize),'little')

                    key_r_len = int.from_bytes(self._file.read(self.db.keysize_bytesize),'little')
                    key_r = self._file.read(self.db.keysize)[:key_r_len].decode('ascii')

                    if key_r == key:

                        if data_occupance_info in [OCCUPANCE_OCCUPIED,OCCUPANCE_OCCUPIED_COLLIDED]:

                            self._file.seek(self.db.data_location+cur_dataindex)
                            self._file.write({OCCUPANCE_OCCUPIED:OCCUPANCE_NOT_OCCUPIED,OCCUPANCE_OCCUPIED_COLLIDED:OCCUPANCE_NOT_OCCUPIED_COLLIDED}[data_occupance_info])
                            break

                        else:

                            raise ValueError("Couldn't find key that you were trying to delete")

                    else:

                        if data_occupance_info == OCCUPANCE_OCCUPIED_COLLIDED:

                            cur_dataindex = collided_index

                        elif data_occupance_info == OCCUPANCE_NOT_OCCUPIED:

                            raise ValueError("Couldn't find key that you were trying to delete")

                        elif data_occupance_info == OCCUPANCE_OCCUPIED:

                            raise ValueError("Couldn't find key that you were trying to delete")

                        elif data_occupance_info == OCCUPANCE_NOT_OCCUPIED_COLLIDED:

                            if collided_index == o_dataindex: # To evade infinite looping

                                raise ValueError("Couldn't find key that you were trying to delete")

                            cur_dataindex = collided_index

    def set(self,key,data):

        self.db.log(f"SET {key}")

        comp_data = self.db.structure.compile(data)

        slot_index = self.db.get_slot_index(key)

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

            o_dataindex = int.from_bytes(self._file.read(self.db.indexsize),'little')

            self._file.seek(self.db.data_location+o_dataindex)

            data_occupance_info =  self._file.read(1)

            if data_occupance_info == OCCUPANCE_NOT_OCCUPIED:

                self._write_data_at(o_dataindex,key,comp_data)

            elif data_occupance_info in [OCCUPANCE_OCCUPIED_COLLIDED,OCCUPANCE_OCCUPIED,OCCUPANCE_NOT_OCCUPIED_COLLIDED]:

                cur_dataindex = o_dataindex

                potential_index = None
                potential_index_original_collider = None

                while True:

                    self._file.seek(self.db.data_location+cur_dataindex)

                    data_occupance_info =  self._file.read(1)

                    collided_index = int.from_bytes(self._file.read(self.db.indexsize),'little')

                    key_r_len = int.from_bytes(self._file.read(self.db.keysize_bytesize),'little')
                    key_r = self._file.read(self.db.keysize)[:key_r_len].decode('utf-8')

                    if key_r == key:

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
                            self._file.write(OCCUPANCE_OCCUPIED_COLLIDED)
                            self._file.write(new_dataindex.to_bytes(self.db.indexsize,"little"))

                            self._write_data_at(new_dataindex,key,comp_data,None if not potential_index else potential_index_original_collider)
                            break

                        elif data_occupance_info == OCCUPANCE_NOT_OCCUPIED_COLLIDED:

                            potential_index = cur_dataindex
                            potential_index_original_collider = collided_index

                            if collided_index == o_dataindex: # To evade infinite looping

                                self._write_data_at(cur_dataindex,key,comp_data,collided_index)
                                break

                            cur_dataindex = collided_index

    def get(self,key):

        self.db.log(f"GET {key}")

        slot_index = self.db.get_slot_index(key)

        self._file.seek(slot_index)
        occupance_info = self._file.read(1)
        self._file.seek(slot_index)

        if occupance_info == OCCUPANCE_NOT_OCCUPIED:

            return None

        elif occupance_info == OCCUPANCE_OCCUPIED:

            self._file.seek(1,1)

            o_dataindex = int.from_bytes(self._file.read(self.db.indexsize),'little')

            self._file.seek(self.db.data_location+o_dataindex)

            data_occupance_info =  self._file.read(1)

            if data_occupance_info == OCCUPANCE_NOT_OCCUPIED:

                return None

            elif data_occupance_info in [OCCUPANCE_OCCUPIED_COLLIDED,OCCUPANCE_OCCUPIED,OCCUPANCE_NOT_OCCUPIED_COLLIDED]:

                cur_dataindex = o_dataindex

                while True:

                    self._file.seek(self.db.data_location+cur_dataindex)

                    data_occupance_info =  self._file.read(1)

                    collided_index = int.from_bytes(self._file.read(self.db.indexsize),'little')

                    key_r_len = int.from_bytes(self._file.read(self.db.keysize_bytesize),'little')
                    key_r = self._file.read(self.db.keysize)[:key_r_len].decode('ascii')

                    if key_r == key:

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

                        elif data_occupance_info == OCCUPANCE_NOT_OCCUPIED_COLLIDED:

                            if collided_index == o_dataindex: # To evade infinite looping

                                return None

                            cur_dataindex = collided_index

    def has(self,key):

        self.db.log(f"HAS {key}")

        slot_index = self.db.get_slot_index(key)

        self._file.seek(slot_index)
        occupance_info = self._file.read(1)
        self._file.seek(slot_index)

        if occupance_info == OCCUPANCE_NOT_OCCUPIED:

            return False

        elif occupance_info == OCCUPANCE_OCCUPIED:

            self._file.seek(1,1)

            o_dataindex = int.from_bytes(self._file.read(self.db.indexsize),'little')

            self._file.seek(self.db.data_location+o_dataindex)

            data_occupance_info =  self._file.read(1)

            if data_occupance_info == OCCUPANCE_NOT_OCCUPIED:

                return False

            elif data_occupance_info in [OCCUPANCE_OCCUPIED_COLLIDED,OCCUPANCE_OCCUPIED,OCCUPANCE_NOT_OCCUPIED_COLLIDED]:

                cur_dataindex = o_dataindex

                while True:

                    self._file.seek(self.db.data_location+cur_dataindex)

                    data_occupance_info =  self._file.read(1)

                    collided_index = int.from_bytes(self._file.read(self.db.indexsize),'little')

                    key_r_len = int.from_bytes(self._file.read(self.db.keysize_bytesize),'little')
                    key_r = self._file.read(self.db.keysize)[:key_r_len].decode('ascii')

                    if key_r == key:

                        if data_occupance_info in [OCCUPANCE_OCCUPIED,OCCUPANCE_OCCUPIED_COLLIDED]:

                            return True

                        else:

                            return False

                    else:

                        if data_occupance_info == OCCUPANCE_OCCUPIED_COLLIDED:

                            cur_dataindex = collided_index

                        elif data_occupance_info == OCCUPANCE_NOT_OCCUPIED:

                            return False

                        elif data_occupance_info == OCCUPANCE_OCCUPIED:

                            return False

                        elif data_occupance_info == OCCUPANCE_NOT_OCCUPIED_COLLIDED:

                            if collided_index == o_dataindex: # To evade infinite looping

                                return False

                            cur_dataindex = collided_index

    def find(self,entryvalue,operator,queryvalue,findmode="all"):

        """
        Operator is either one of the default operators: has / contains, in, equal to / equal, not equal to, endswith, startswith, less than, greater than, modulo
        OR
        a callable function taking 2 arguments: value1, value2 -> value1 being the value found in the database and value2 the provided value.
        The function should return a boolean.

        Default operators like less than compare found value with provided value
        eg. You're looking for all posts with less than 20 likes
                db.find("likes","less than",20)

        Some symbol versions of the operators are also included like: %, <, >, <=, == or = , ...

        This also means that the ' in ' operator doesn't check wether the provided value is in the value in the database but it checks wether the database value is in the provided value.
        eg. You're looking for all entries whose title is in a list of books
                db.find("title","in",["The Lord of The Rings","The Hobbit","Guide to Programming: Vol. 1"])

        Keep in mind that the find function doesn't check for datatypes so checking if a string value is less than something (or reversed) will raise an error

        There are 2 findmodes:
        ' all ' : returns a list of all valid keys
        ' first ' : returns first valid key (not neccecarily in order of insertion) or None

        Returns a list of keys
        """

        op = find_operators.default[operator.strip().lower()] if operator.strip().lower() in find_operators.default else operator

        start_pos = self.db.data_location

        v_offset = self.db.structure.get_value_offset(entryvalue)

        self._file.seek(start_pos+1)

        ks = []

        while True:

            begin = self._file.tell()

            oi = self._file.read(1)

            if oi in [OCCUPANCE_OCCUPIED,OCCUPANCE_OCCUPIED_COLLIDED]:

                self._file.seek(begin+1+self.db.indexsize+self.db.keysize_bytesize+self.db.keysize+v_offset)

                v = self.db.structure.fetch_value_here(entryvalue, self._file)

                if op(v,queryvalue):

                    self._file.seek(begin+1+self.db.indexsize)
                    kl = int.from_bytes(self._file.read(self.db.keysize_bytesize),'little')
                    k = self._file.read(kl).decode('ascii')

                    if findmode == "all":
                        ks.append(k)
                    else:
                        return k

            self._file.seek(begin)
            self._file.seek(self.db.entry_size,1)

            if self._file.tell() >= int(os.path.getsize(self.db.location))-1:

                break

        return ks if findmode == "all" else None

    def find_generator(self,entryvalue,operator,queryvalue):

        """

        Same as Accessor.find except findmode obviously doesn't apply

        """

        op = find_operators.default[operator.strip()] if operator.strip() in find_operators.default else operator

        start_pos = self.db.data_location

        v_offset = self.db.structure.get_value_offset(entryvalue)

        self._file.seek(start_pos+1)

        ks = []

        while True:

            begin = self._file.tell()

            oi = self._file.read(1)

            if oi in [OCCUPANCE_OCCUPIED,OCCUPANCE_OCCUPIED_COLLIDED]:

                self._file.seek(begin+1+self.db.indexsize+self.db.keysize_bytesize+self.db.keysize+v_offset)

                v = self.db.structure.fetch_value_here(entryvalue, self._file)

                if op(v,queryvalue):

                    self._file.seek(begin+1+self.db.indexsize)
                    kl = int.from_bytes(self._file.read(self.db.keysize_bytesize),'little')
                    k = self._file.read(kl).decode('ascii')

                    yield k

            self._file.seek(begin)
            self._file.seek(self.db.entry_size,1)

            if self._file.tell() >= int(os.path.getsize(self.db.location))-1:

                break

class WrongMagicNum(Exception):

    pass
