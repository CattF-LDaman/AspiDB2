

def contains(value1,value2):

    if value2 in value1:

        return True

    return False

def asp_in(value1,value2):

    if value1 in value2:

        return True

    return False

def equal(value1,value2):

    if value2 == value1:

        return True

    return False

def notequal(value1,value2):

    if value2 != value1:

        return True

    return False

def endswith(value1,value2):

    if value1.endswith(value2):

        return True

    return False

def startswith(value1,value2):

    if value1.startswith(value2):

        return True

    return False

def lessthan(value1,value2):

    if value1 < value2:

        return True

    return False

def greaterthan(value1,value2):
    if value1 > value2:

        return True

    return False

def lessthanequal(value1,value2):

    if value1 <= value2:

        return True

    return False

def greaterthanequal(value1,value2):

    if value1 >= value2:

        return True

    return False

default = {

    "contains": contains,
    "has": contains,
    "in": asp_in,
    "equal": equal,
    "is": equal,
    "equal to": equal,
    "not equal": notequal,
    "not equal to": notequal,
    "notequal": notequal,
    "endswith": endswith,
    "ends with": endswith,
    "startswith": startswith,
    "starts with": startswith,
    "less": lessthan,
    "less than": lessthan,
    "lessthan": lessthan,
    "greater": greaterthan,
    "greater than": greaterthan,
    "greatherthan": greaterthan,
    ">":greaterthan,
    "<":lessthan,
    "==":equal,
    "=":equal,
    "<=":lessthanequal,
    ">=":greaterthanequal,
    "less than or equal to":lessthanequal,
    "less than or equal":lessthanequal,
    "greater than or equal":greaterthanequal,
    "greater than or equal to":greaterthanequal,
}
