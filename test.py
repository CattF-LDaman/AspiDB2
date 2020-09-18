from Aspi2 import database

def test():

    test_struc = (("firstname","UnicodeString",{"size":64},False),("lastname","UnicodeString",{"size":128},False),("age","IntUnsigned",{"bitsize":8},True))

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

    test_struc = (("firstname","UnicodeString",{"size":64},False),("lastname","UnicodeString",{"size":128},False),("age","IntUnsigned",{"bitsize":8},True))

    database.build("TestDB.asp2", "TestDB", test_struc)

    db = database.Database("TestDB.asp2")

    db_accessor  = database.Accessor(db)

    db_accessor.set("jsmith",{"firstname":"John","lastname":"Smith","age":35})
    db_accessor.set("rocketman",{"firstname":"Elton","lastname":"John","age":72})
    db_accessor.set("macca",{"firstname":"Sir Paul","lastname":"Mccartney","age":78})

    print(db_accessor.find("firstname", "has", "Paul"))

if __name__ == "__main__":

    find_test()
