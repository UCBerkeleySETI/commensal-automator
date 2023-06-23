import logging

LOG_FORMAT = "[%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s] %(message)s"
LOGGER_NAME = "BLUSE.interface"

log = logging.getLogger(LOGGER_NAME)

def set_logger(level=logging.DEBUG):
    """Set logging format.

    Args:
        level (str): Logging level (see logging module). 

    Returns:
        log (obj): The logger. 
    """

    logging.basicConfig(format=LOG_FORMAT)
    log.setLevel(level)
    return log

