
import os
import hashlib
import time
import shutil

from . import MAGIC_NUM,VERSION
from . import logging_utils
from . import math_utils
from . import find_operators
from . import config
from . import structure
from . import caching

def build(location,name,struc,slots=1024,max_key_length=64,index_size_bytes=12):

    with open(location,"wb") as f:

        f.write(MAGIC_NUM)

        f.write(len(name.encode('utf-8')).to_bytes(2,"little"))
        f.write(name.encode('utf-8'))

        f.write(VERSION.to_bytes(3,'little'))

        f.write(max_key_length.to_bytes(2,'little'))
        f.write(index_size_bytes.to_bytes(1,'little'))

        c_struc = structure.compile_structure(struc)
        f.write(len(c_struc).to_bytes(4,'little'))
        f.write(c_struc)

        f.write(slots.to_bytes(12,'little'))

        f.seek(slots*(index_size_bytes+1),1)
        f.write(b"\x00")

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
            self._struc_raw = structure.decompile_struc(struc_bytes)
            self.structure = structure.load_structure(struc_bytes)

            self._slotsize = 1 + self.indexsize # occupance info (1B) + index

            self.config = config.load(db_config)

            self.slots = int.from_bytes(db_f.read(12),'little')

            self.indices_location = db_f.tell()

            db_f.seek(self.slots*self._slotsize,IO_WHENCE_RELATIVE)

            self.data_location = db_f.tell()

        self.entry_size = 1+self.indexsize+self.keysize_bytesize+self.keysize+len(self.structure)

        self.logger = logging_utils.Logger(self.name, self.config['logger_directory'].format(dbname=self.name), self.config["logger_enabled"], self.config["logger_print_enabled"], self.config["logger_print_level"])
        self.log = self.logger.log

        self.accessors = []

        self.log(f"Database ' {self.name} ' initialised with version ' {self.version} ' (Client version is ' {VERSION} '),  structure of size {len(self.structure)} ( {len(self.structure)/1024/1024} mb ), at most {int(2**(self.indexsize*8)/self.entry_size)} entries possible.")

    def get_slot(self,key):

        return int.from_bytes(hashlib.sha1(key.encode('ascii')).digest(),'little') % self.slots

    def get_slot_index(self,key):

        return self.indices_location + self.get_slot(key) * self._slotsize

    def backup(self,backup_identifier="manual"):

        if not os.path.exists(self.config["backup_directory"].format(dbname=self.name)):

            os.makedirs(self.config["backup_directory"].format(dbname=self.name),exist_ok=True)
            self.log(f"Made new directory ' {self.config['backup_directory'].format(dbname=self.name)} '")

        shutil.copyfile(self.location, os.path.join(self.config["backup_directory"].format(dbname=self.name),f"backup_{backup_identifier}_{int(time.time())}_{self.slots}.asp2"))

    def close_all_accessors(self):

        for a in self.accessors:

            a.close()

    def rescale(self,new_slot_amount=None):

        self.log("RESCALE")

        self.close_all_accessors()

        rescale_start_time = time.time()

        health_before = self.health

        rescale_job_id = f"{int(rescale_start_time)}&{os.urandom(6).hex()}"

        db_len = len(self)
        target_slots = db_len*4 if not new_slot_amount else new_slot_amount
        if target_slots == self.slots:
            self.log("RESCALE: Rescale cancelled. Already at target slot amount.")
            return

        self.log(f"RESCALE: New slot amount will be {target_slots} slots")

        rescale_db_path = f"rescale_{rescale_job_id}.rasp2"

        build(rescale_db_path, self.name, self._struc_raw, target_slots, self.keysize, self.indexsize)

        rescale_db = Database(rescale_db_path, {"logger_enabled":False})
        rescale_db_accessor = Accessor(rescale_db)
        db_accessor = Accessor(self)

        for k,v in db_accessor.items():

            rescale_db_accessor.set(k,v)

        if self.config["rescale_backups"]:

            self.log("RESCALE: Backing up...")
            self.backup("rescale")
            self.log("RESCALE: Done backing up!")

        db_accessor.close()
        rescale_db_accessor.close()

        self.log("RESCALE: Overwriting current db")
        shutil.copyfile(rescale_db_path,self.location)

        self.log("RESCALE: Deleting rescale db")
        os.remove(rescale_db_path)

        self.slots = target_slots

        health_after = self.health

        improvement = health_after-health_before

        self.log(f"RESCALE: Health before: {health_before}, health after : {health_after}, improvement: {improvement}")

        return improvement

    @property
    def health(self):

        return Accessor(self).health

    def __len__(self):

        return len(Accessor(self))

class Accessor:

    def __init__(self,database):

        self.db = database
        self.db.accessors.append(self)

        self.db.log("Accessor created.","DEBUG")

        self._file =  open(self.db.location,"rb+")

        self._val_cache = caching.Cache(self.db.config["max_cache_size"])
        self._health_cache = None
        self._len_cache = None

        self.closed = False

    def close(self):

        self.closed = True
        self.db.accessors.remove(self)
        self._file.close()

    def all_keys(self):

        return list(self.keys())

    def all_values(self):

        return list(self.values())

    def all_items(self):

        return list(self.items())

    def _write_data_at(self,index,key,data,collider=None):

        self._file.seek(self.db.data_location+index)

        self._file.write(OCCUPANCE_OCCUPIED if not collider else OCCUPANCE_OCCUPIED_COLLIDED)
        self._file.write(collider.to_bytes(self.db.indexsize,'little') if collider else b"\x00"*self.db.indexsize)
        self._file.write(len(key).to_bytes(self.db.keysize_bytesize,'little'))
        self._file.write(key.encode("ascii")+(self.db.keysize-len(key))*b"\x00")
        self._file.write(data)

        self._val_cache.set(key,data)

    def delete(self,key):

        self.db.log(f"DELETE {key}","DEBUG")

        self._len_cache = None
        self._health_cache = None

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

        self._val_cache.invalidate(key)

    def set(self,key,data):

        self.db.log(f"SET {key}","DEBUG")

        self._len_cache = None
        self._health_cache = None

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

        self.db.log(f"GET {key}","DEBUG")

        if self._val_cache.has(key):

            return self.db.structure.fetch(self._val_cache.get(key))

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

        self.db.log(f"HAS {key}","DEBUG")

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

        self.db.log(f"FIND {findmode.upper().strip()} ENTRY(/-IES) WHERE {entryvalue} {operator} {queryvalue}","DEBUG")

        findmode = findmode.lower().strip()

        """
        Operator is either one of the default operators: has / contains, in, equal to / equal, not equal to, endswith, startswith, less than, greater than, modulo
        OR
        a callable function taking 2 arguments: value1, value2 -> value1 being the value found in the database and value2 the provided value.
        The function should return a boolean.

        Default operators like less than compare found value with provided value
        eg. You're looking for all posts with less than 20 likes
                db.find("likes","less than",20)

        Some symbol versions of the operators are also included like: <, >, <=, == or = , ...

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

        start_pos = self.db.data_location+1

        v_offset = self.db.structure.get_value_offset(entryvalue)

        self._file.seek(start_pos)

        ks = []

        while self._file.tell() <= int(os.path.getsize(self.db.location))-1:

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


        return ks if findmode == "all" else None

    def find_generator(self,entryvalue,operator,queryvalue):

        self.db.log(f"FIND ALL ENTRIES WHERE {entryvalue} {operator} {queryvalue}","DEBUG")

        """

        Same as Accessor.find except findmode obviously doesn't apply

        """

        op = find_operators.default[operator.strip()] if operator.strip() in find_operators.default else operator

        start_pos = self.db.data_location+1

        v_offset = self.db.structure.get_value_offset(entryvalue)

        self._file.seek(start_pos)

        ks = []

        while self._file.tell() <= int(os.path.getsize(self.db.location))-1:

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

    def keys(self):

        self.db.log("KEYS","DEBUG")

        start_pos = self.db.data_location+1

        self._file.seek(start_pos)

        cur_pos = start_pos

        while self._file.tell() <= int(os.path.getsize(self.db.location))-1:

            oi = self._file.read(1)

            if oi in [OCCUPANCE_OCCUPIED,OCCUPANCE_OCCUPIED_COLLIDED]:

                self._file.seek(self.db.indexsize,1)

                kl = int.from_bytes(self._file.read(self.db.keysize_bytesize),'little')
                k = self._file.read(kl).decode('ascii')

                yield k

            cur_pos += self.db.entry_size
            self._file.seek(cur_pos)

    def values(self):

        self.db.log("VALUES","DEBUG")

        start_pos = self.db.data_location+1

        self._file.seek(start_pos)

        cur_pos = start_pos

        while self._file.tell() <= int(os.path.getsize(self.db.location))-1:

            oi = self._file.read(1)

            if oi in [OCCUPANCE_OCCUPIED,OCCUPANCE_OCCUPIED_COLLIDED]:

                self._file.seek(self.db.indexsize,1)


                self._file.seek(self.db.keysize_bytesize+self.db.keysize,1)

                v = self.db.structure.fetch(self._file.read(len(self.db.structure)))

                yield v

            cur_pos += self.db.entry_size
            self._file.seek(cur_pos)

    def items(self):

        self.db.log("ITEMS","DEBUG")

        start_pos = self.db.data_location+1

        self._file.seek(start_pos)

        cur_pos = start_pos

        while self._file.tell() <= int(os.path.getsize(self.db.location))-1:

            oi = self._file.read(1)

            if oi in [OCCUPANCE_OCCUPIED,OCCUPANCE_OCCUPIED_COLLIDED]:

                self._file.seek(self.db.indexsize,1)

                kl = int.from_bytes(self._file.read(self.db.keysize_bytesize),'little')
                k = self._file.read(kl).decode('ascii')

                self._file.seek(self.db.keysize-kl,1)
                v = self.db.structure.fetch(self._file.read(len(self.db.structure)))

                yield (k,v)

            cur_pos += self.db.entry_size
            self._file.seek(cur_pos)

    def __len__(self):

        self.db.log("LENGTH","DEBUG")

        if self._len_cache is not None:
            return self._len_cache

        start_pos = self.db.data_location+1

        self._file.seek(start_pos)

        cur_pos = start_pos

        length = 0

        while self._file.tell() <= int(os.path.getsize(self.db.location))-1:

            oi = self._file.read(1)

            if oi in [OCCUPANCE_OCCUPIED,OCCUPANCE_OCCUPIED_COLLIDED]:

                length += 1

            cur_pos += self.db.entry_size
            self._file.seek(cur_pos)

        self._len_cache = length

        return length

    @property
    def length(self):

        return len(self)

    @property
    def health(self):

        self.db.log("HEALTH","DEBUG")

        if self._health_cache is not None:
            return self._health_cache

        start_pos = self.db.data_location+1

        self._file.seek(start_pos)

        cur_pos = start_pos

        collided = 0
        not_collided = 0

        while self._file.tell() <= int(os.path.getsize(self.db.location))-1:

            oi = self._file.read(1)

            if oi in [OCCUPANCE_OCCUPIED_COLLIDED,OCCUPANCE_NOT_OCCUPIED_COLLIDED]:

                collided += 1

            else:

                not_collided += 1

            cur_pos += self.db.entry_size
            self._file.seek(cur_pos)

        try:
            health = (1-collided/(not_collided+collided))*100
        except ZeroDivisionError:
            health =  100

        self._health_cache = health

        return health

class WrongMagicNum(Exception):

    pass
