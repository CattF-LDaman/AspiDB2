
import os

from . import MAGIC_NUM
from . import logging_utils

class Database:

    def __init__(location):

        self.location = location

        if not os.path.isfile(location):

            raise FileNotFoundError(f"Didn't find file at '{location}'")

        with open(location,"rb") as db_f:

            if db_f.read(len(MAGIC_NUM)) != MAGIC_NUM:

                raise WrongMagicNum("Didn't find magic number at beginning of file at location")

            db_f.read()

class WrongMagicNum(Exception):

    pass
