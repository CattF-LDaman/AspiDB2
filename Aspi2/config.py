
DEFAULT_CONFIG = {

    "logger_enabled":True,
    "logger_directory":"{dbname}_logs",
    "logger_print_enabled":True,
    "logger_print_level":"INFO",

    "rescale_backups":True,
    "backup_directory":"{dbname}_backups",

    "max_cache_size":1024*1024
}

def load(c):

    return { **DEFAULT_CONFIG, **c }
