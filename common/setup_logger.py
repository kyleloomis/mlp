import logging

logger = logging.getLogger()


def setup_logger(verbose: bool = False):
    handler = logging.StreamHandler()
    log_format = logging.Formatter(
        "%(asctime)s [%(levelname)s] - %(name)s: %(message)s"
    )
    handler.setFormatter(log_format)
    logger.addHandler(handler)
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
