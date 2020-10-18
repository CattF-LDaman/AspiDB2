
import math
import struct

class Datatype:

    def __init__(self):

        self.real_type = None

    def _compiler(self,nb):

        raise NotImplementedError

    def compile(self,nb):

        if type(nb) != self.real_type:

            raise ValueError(f"Could not compile: expected type ' {self.real_type} ' got  ' {type(nb)} '")

        return self._compiler(nb)

    def _fetcher(self,b):

        raise NotImplementedError

    def fetch(self,b):

        assert self.validate(b), "Could not validate"

        return self._fetcher(b)

    def _validator(self,b):

        raise NotImplementedError

    def validate(self,b):

        if len(b) != len(self):

            return False

        return self._validator(b)

    def __len__(self):

        return 0

class IntUnsigned(Datatype):

    def __init__(self,size=1):

        self.real_type = int

        self.bytesize = size

    def _compiler(self,nb):

        return nb.to_bytes(self.bytesize, 'little')

    def _validator(self,b):

        return True

    def _fetcher(self,b):

        return int.from_bytes(b,'little')

    def __len__(self):

        return self.bytesize

class IntSigned(Datatype):

    def __init__(self,size=1):

        self.real_type = int

        self.bytesize = size

    def _compiler(self,nb):

        return nb.to_bytes(self.bytesize, 'little',signed=True)

    def _validator(self,b):

        return True

    def _fetcher(self,b):

        return int.from_bytes(b,'little',signed=True)

    def __len__(self):

        return self.bytesize

class FixedBytes(Datatype):

    def __init__(self,size=1):

        self.real_type = bytes

        self.bytesize = size

    def _compiler(self,nb):

        return nb

    def _validator(self,b):

        return True

    def _fetcher(self,b):

        return b

    def __len__(self):

        return self.bytesize

class Bytes(Datatype):

    def __init__(self,size=1):

        self.real_type = bytes

        self._lenbytesize = int(math.log(size)/ math.log(2)/8)+1
        self.bytesize = size+self._lenbytesize

        self.max_bchars = size

    def _compiler(self,nb):

        if len(nb) > self.max_bchars:

            raise ValueError("Can't compile: length exceeds maximum byte amount")

        bl = len(nb).to_bytes(self._lenbytesize,'little')
        return bl+nb+b"".join([b"\x00" for _ in range(self.bytesize-(len(nb)+len(bl)))])

    def _validator(self,b):

        return True

    def _fetcher(self,b):

        lb = b[:self._lenbytesize]
        ebp = b[self._lenbytesize:]
        eb = ebp[:int.from_bytes(lb,'little')]
        return eb

    def __len__(self):

        return self.bytesize

class ASCIIString(Datatype):

    def __init__(self,size=1):

        self.real_type = str

        self._lenbytesize = int(math.log(size)/ math.log(2)/8)+1
        self.bytesize = size+self._lenbytesize

        self.max_chars = size

    def _compiler(self,nb):

        if len(nb) > self.max_chars:

            raise ValueError("Can't compile: length exceeds maximum char amount")

        eb = nb.encode('ascii')
        bl = len(eb).to_bytes(self._lenbytesize,'little')
        return bl+eb+b"".join([b"\x00" for _ in range(self.bytesize-(len(eb)+len(bl)))])

    def _validator(self,b):

        return True

    def _fetcher(self,b):

        lb = b[:self._lenbytesize]
        ebp = b[self._lenbytesize:]
        eb = ebp[:int.from_bytes(lb,'little')]
        return eb.decode('ascii')

    def __len__(self):

        return self.bytesize

class UnicodeString(Datatype):

    def __init__(self,size=1):

        self.real_type = str

        self._lenbytesize = int(math.log(size*4)/ math.log(2)/8)+1
        self.bytesize = size*4+self._lenbytesize

        self.max_chars = size

    def _compiler(self,nb):

        if len(nb) > self.max_chars:

            raise ValueError("Can't compile: length exceeds maximum char amount")

        eb = nb.encode('utf-8')
        bl = len(eb).to_bytes(self._lenbytesize,'little')
        return bl+eb+b"".join([b"\x00" for _ in range(self.bytesize-(len(eb)+len(bl)))])

    def _validator(self,b):

        return True

    def _fetcher(self,b):

        lb = b[:self._lenbytesize]
        ebp = b[self._lenbytesize:]
        eb = ebp[:int.from_bytes(lb,'little')]
        return eb.decode('utf-8')

    def __len__(self):

        return self.bytesize

class Boolean(Datatype):

    def __init__(self):

        self.real_type = bool

    def _compiler(self,nb):

        return b"\x00" if not nb else b"\x01"

    def _validator(self,b):

        return False if b not in [b"\x00",b"\x01"] else True

    def _fetcher(self,b):

        return True if b == "\x01" else False

    def __len__(self):

        return 1

def Decimal(Datatype):

    def __init__(self):

        self.real_type = bytes

    def _compiler(self,fl):

        return struct.pack("<d",fl)

    def _validator(self,b):

        return True

    def _fetcher(self,b):

        return struct.unpack("<d",fl)[0]

    def __len__(self):

        return 8

TYPES = {

    "IntUnsigned":IntUnsigned,
    "IntSigned":IntSigned,
    "FixedBytes":FixedBytes,
    "Bytes":Bytes,
    "ASCIIString":ASCIIString,
    "UnicodeString":UnicodeString,
    "Boolean":Boolean,
    "Decimal":Decimal,
    "Float":Decimal,
    "String":UnicodeString,
    "Int":IntSigned

}
