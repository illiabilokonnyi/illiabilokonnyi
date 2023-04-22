"""
This module contains a start method that creates necessary
directories, starts logging and enables the endpoint.
"""
import logging
from datetime import datetime

from endpoint import run_app, app


def manage_tmp_dirs():
    ...


def start() -> None:
    """
    This method starts logging and enables the endpoint.
    :return: None
    """

    # enabling logging
    logging.basicConfig(
        level=logging.ERROR,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("./logs/debug.log"),
            logging.StreamHandler()
        ]
    )
    logging.debug('started [%s]', datetime.now())

    # starting the app
    run_app()


def get_app():
    return app
