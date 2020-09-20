
from . import datatypes

import json
import io
import math
from inspect import getargs
import bitarray
from collections import OrderedDict

def validate_structure(struc:tuple):

    for index ,(keyname,datatype,arguments,nullable) in enumerate(struc):

        if datatype not in datatypes.TYPES:

            raise UnknownDatatypeError(f"Identifier '{datatype}' in struc argument not found in known datatypes")

        dto = datatypes.TYPES[datatype]

        dto(**arguments)

def compile_structure(struc):

    o = b""

    o += len(struc).to_bytes(4, "little")
    for keyname,datatype,arguments,nullable in struc:

        o += len(keyname.encode('ascii')).to_bytes(2,'little')
        o += keyname.encode('ascii')
        o += len(datatype.encode('ascii')).to_bytes(1,'little')
        o += datatype.encode('ascii')
        o += len(arguments).to_bytes(1, "little")
        for arg,val in arguments.items():
            o += len(arg).to_bytes(1, "little")
            o += arg.encode("ascii")
            valenc = json.dumps(val).encode('utf-8')
            o += len(valenc).to_bytes(2,"little")
            o += valenc
        o += b"\x01" if nullable else b"\x00"

    return o

def load_structure(bstruc):

    o = []
    b = io.BytesIO(bstruc)

    for _ in range(int.from_bytes(b.read(4),'little')):

        do = []

        knl = int.from_bytes(b.read(2),'little')
        do.append(b.read(knl).decode('ascii'))

        dtl = int.from_bytes(b.read(1),'little')
        do.append(b.read(dtl).decode('ascii'))

        args = {}
        arg_amount = int.from_bytes(b.read(1),'little')
        for _ in range(arg_amount):

            arglen = int.from_bytes(b.read(1),'little')
            arg = b.read(arglen).decode('ascii')

            vall = int.from_bytes(b.read(2),'little')
            val = json.loads(b.read(vall).decode('utf-8'))

            args[arg] = val
        do.append(args)
        do.append(True if b.read(1) == b"\x01" else False)

        o.append(do)

    return Structure(tuple(o))

class Structure:

    def __init__(self,struc:tuple):

        validate_structure(struc)

        self.struc_raw = struc

        self.keys = OrderedDict((keyname,index) for index, (keyname,_,_2,_3) in enumerate(struc))
        self.data = tuple([datatypes.TYPES[datatype](**arguments) for _,datatype, arguments, nullable in struc])
        self.nullables= tuple([nullable for _,_2,_3,nullable in struc])

        self.nulmap_size = int(math.ceil((len(self.data))/8))

        self._value_offset_cache = {}

    def __len__(self):

        return sum([len(do) for do in self.data])+self.nulmap_size

    def fetch_value_here(self,valuename,fileobj):

        vdatatype = self.data[self.keys[valuename]]

        valuelen = len(vdatatype)

        valuedata = fileobj.read(valuelen)

        fileobj.seek(-valuelen,1)

        return vdatatype.fetch(valuedata)

    def get_value_offset(self,valuename):

        if valuename in self._value_offset_cache:

            return self._value_offset_cache[valuename]

        if valuename not in self.keys:

            raise ValueError("Given value not in structure")

        lb = 0

        for keyn,index in self.keys.items():

            if keyn == valuename:
                break

            lb += len(self.data[index])

        self._value_offset_cache[valuename] = self.nulmap_size+lb

        return self.nulmap_size+lb

    def compile(self,sdata):

        nulmap = bitarray.bitarray("0" * len(self.data),endian="little")
        d_out = b""

        for keyn,index in self.keys.items():

            if keyn not in sdata or sdata[keyn] is None:

                if not self.nullables[index]:

                    raise ValueCanNotBeNullError("Given data is missing a value that is not allowed to be null.")

                nulmap[index] = True
                d_out += b"\x00"*len(self.data[index])

            else:

                d_out += self.data[index].compile(sdata[keyn])

        return nulmap.tobytes()+d_out

    def fetch(self,sdata):

        nulmap = bitarray.bitarray(endian='little')
        nulmap.frombytes(sdata[:self.nulmap_size])

        d = {}
        d_in = sdata[self.nulmap_size:]

        cur_cursor = 0
        prev_cursor = 0

        for keyn,index in self.keys.items():

            if nulmap[index]:

                if not self.nullables[index]:

                    raise ValueCanNotBeNullError("Value in fetched data is null but is not allowed to be null")

                d[keyn] = None
                continue

            datatype = self.data[index]
            read_len = len(datatype)

            cur_cursor += read_len
            valuedata = d_in[prev_cursor:cur_cursor]
            prev_cursor = cur_cursor
            d[keyn] = datatype.fetch(valuedata)

        return d

class UnknownDatatypeError(Exception):

    pass

class ValueCanNotBeNullError(Exception):

    pass
