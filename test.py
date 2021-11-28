from Aspi2 import database

import threading
import random

def test():

    test_struc = (("firstname","UnicodeString",{"size":64},False),("lastname","UnicodeString",{"size":128},False),("age","IntUnsigned",{"size":1},True))

    database.build("TestDB.asp2", "TestDB", test_struc)

    db = database.Database("TestDB.asp2")

    db_accessor  = database.Accessor(db)

    db_accessor.set("jsmith",{"firstname":"John","lastname":"Smith","age":35})

    print(db_accessor.get("jsmith"))
    print(db_accessor.has("jsmith"))

    db_accessor.delete("jsmith")

    print(db_accessor.get("jsmith"))
    print(db_accessor.has("jsmith"))

def find_test():

    test_struc = (("firstname","UnicodeString",{"size":64},False),("lastname","UnicodeString",{"size":128},False),("age","IntUnsigned",{"size":1},True))

    database.build("TestDB.asp2", "TestDB", test_struc)

    db = database.Database("TestDB.asp2")

    db_accessor  = database.Accessor(db)

    db_accessor.set("jsmith",{"firstname":"John","lastname":"Smith","age":35})
    db_accessor.set("rocketman",{"firstname":"Elton","lastname":"John","age":72})
    db_accessor.set("macca",{"firstname":"Sir Paul","lastname":"Mccartney","age":78})

    print(db_accessor.find("firstname", "has", "Paul"))

def threaded_test():

        test_struc = (("firstname","UnicodeString",{"size":64},False),("lastname","UnicodeString",{"size":128},False),("age","IntUnsigned",{"size":1},True))

        database.build("TestDB.asp2", "TestDB", test_struc)

        db = database.Database("TestDB.asp2")

        def _dbthread():

            thr_accessor = database.Accessor(db)

            c = [{"firstname":"John","lastname":"Smith","age":35},{"firstname":"Elton","lastname":"John","age":72},{"firstname":"Sir Paul","lastname":"Mccartney","age":78}]

            for _ in range(1000):

                thr_accessor.set("person", random.choice(c))

        th1 = threading.Thread(target=_dbthread)
        th2 = threading.Thread(target=_dbthread)
        th3 = threading.Thread(target=_dbthread)

        th1.start()
        th2.start()
        th3.start()
        th3.join()
        th2.join()
        th1.join()

        print(database.Accessor(db).get("person"))

def extra_test():

        test_struc = (("firstname","UnicodeString",{"size":64},False),("lastname","UnicodeString",{"size":128},False),("age","IntUnsigned",{"size":1},True))

        database.build("TestDB.asp2", "TestDB", test_struc)

        db = database.Database("TestDB.asp2")

        db_accessor  = database.Accessor(db)

        db_accessor.set("jsmith",{"firstname":"John","lastname":"Smith","age":35})
        db_accessor.set("rocketman",{"firstname":"Elton","lastname":"John","age":72})
        db_accessor.set("macca",{"firstname":"Sir Paul","lastname":"Mccartney","age":78})

        print(db_accessor.all_values)
        print(db_accessor.all_keys)
        print(db_accessor.all_items)

        print(len(db_accessor))
        print(db_accessor.health())

def large_test():

    test_struc = (("firstname","UnicodeString",{"size":64},False),("lastname","UnicodeString",{"size":128},False),("age","IntUnsigned",{"size":1},True))

    database.build("TestDB.asp2", "TestDB", test_struc, 11000)

    db = database.Database("TestDB.asp2")

    db_accessor  = database.Accessor(db)

    def rnd_string(slen):

        return "".join(random.choice(list("abcdefghijklmnopqrstuvwxyz1234567890")) for _ in range(slen))

    for _ in range(5000):

        db_accessor.set(rnd_string(32),{"firstname":rnd_string(32),"lastname":rnd_string(64),"age":random.randint(0, 100)})
    print("Done adding 5000 users.")

    db_accessor.set("jsmith",{"firstname":"John","lastname":"Smith","age":35})
    print("Done adding John")

    for _ in range(5000):

        db_accessor.set(rnd_string(32),{"firstname":rnd_string(32),"lastname":rnd_string(64),"age":random.randint(0, 100)})
    print("Done adding 5000 users.")

    print(db.health, len(db_accessor))
    print(db_accessor.get("jsmith"))

    print(db_accessor.find("firstname", "has", "Jo"))

    db.rescale()

def dataset_test():

    dataset_struc = (("Gemeente","UnicodeString",{"size":256},False),("Naam","UnicodeString",{"size":128},False))

    #database.build("TestDB.asp2", "DatasetNamen", dataset_struc, 775056)

    db = database.Database("TestDB.asp2")

    db_accessor  = database.Accessor(db)

    #with open("test_dataset.txt","r", encoding="cp1252") as f:
    #    for linen,line in enumerate(f.readlines()):
    #        s = line.split("|")
    #        db_accessor.set(str(linen),{"Gemeente":s[1], "Naam":s[3]})

    print(db_accessor.health)
    print(db_accessor.get(db_accessor.find("Gemeente","is","Jabbeke","first")))

if __name__ == "__main__":

    dataset_test()
