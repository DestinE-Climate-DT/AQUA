# Dummy script, does nothing, used for tests
from aqua.logger import log_configure

if __name__ == '__main__':
    logger = log_configure(log_level='debug', log_name='Dummy CLI')
    logger.info("This is a dummy CLI script that does nothing.")
    exit(0)
