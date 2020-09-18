import math

def bytes_needed_to_store_num(num):

    return math.ceil(math.log2(num)/8)

def next_power_of_two(num):

    return 1<<(num-1).bit_length()
