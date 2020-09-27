
DEFAULT_CONFIG = {

    "logger_enabled":True,
    "logger_directory":"{dbname}_logs",
    "logger_print_enabled":True,
    "logger_print_level":"INFO",

    "rescale_backups":True,
    "backup_directory":"{dbname}_backups"
}

def load(c):

    return { **DEFAULT_CONFIG, **c }
