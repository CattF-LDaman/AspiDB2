
import random

class Cache:

    def __init__(self,maxsize=1024):

        self.maxsize = maxsize
        self._i = {}

    def has(self,key):

        return True if key in self._i else False

    def set(self,key,val):

        self._i[key] = val

        if len(self._i) > self.maxsize:

            self._i.pop(random.choice(list(self._i)))

    def invalidate(self,key):

        self._i.pop(key)

    def get(self,key):

        return self._i[key]
