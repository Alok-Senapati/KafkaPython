import logging


def create_logger(module_name: str = __name__):
    """
    Create a logger
    :param module_name: Name of the module
    :return: logger
    """
    main_logger = logging.getLogger(module_name)
    main_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s : %(levelname)s] - {%(module)s : %(lineno)d} - %(message)s')
    handler.setFormatter(formatter)
    main_logger.addHandler(handler)
    return main_logger
