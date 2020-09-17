import json

DEFAULT_CONFIG = {

    "logger_enabled":True,
    "logger_directory":"logs",
    "logger_print_enabled":True,
    "logger_print_level":"INFO",

    "auto_rescale":False,
    "auto_rescale_minimum_delay":6,

}

def load(b):

    return { **DEFAULT_CONFIG, **json.loads(b.decode('utf-8')) }
