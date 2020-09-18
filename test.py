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

if __name__ == "__main__":

    test()
