"""
A module that consists of supporting functions
allowing to operate with API endpoint modules.
"""

from contextlib import suppress
from functools import wraps
from requests.exceptions import RequestException, HTTPError
from pathlib import Path

import logging
import json

from . import URLS_DIR


def _rectify_default_kwargs(kwds, **kwargs):
    kwds.pop('self', None)
    kwds.pop('kwargs', None)
    kwds.update(kwargs)
    return kwds


def _read_endpoints(module: str, urls_dir: Path = None):
    if not urls_dir:
        urls_dir = URLS_DIR
    with open(urls_dir / f'{module.lower()}.json', 'r') as f:
        endpoint_config = json.load(f)
    base_url = endpoint_config['base_url']
    endpoints = endpoint_config['endpoints']
    return base_url, endpoints


def _read_token(token_dir: str = None, token_name: str = None, token_path: str | Path = None):
    if not token_dir:
        token_dir = './'
    if not token_name:
        token_name = 'token.txt'
    path = token_path or Path(token_dir) / token_name
    with open(path, 'r') as f:
        return f"{f.read().strip().split(':')[0]}"


def raise_for_status(func):
    @wraps(func)
    def wrapped(obj, url, *args, **kwargs):
        try:
            response = func(obj, url, *args, **kwargs)
        except RequestException as exc:
            logging.error(f'Error while processing request {url}')
            logging.debug(exc)
            raise exc
        else:
            try:
                response.raise_for_status()
                return response
            except HTTPError as exc:
                logging.debug(exc)
                if response.text:
                    with suppress(BaseException):
                        response_data = response.json()
                        message = response_data['errors'][0]['message']
                        if not obj.print_errors:
                            raise ValueError(message)
                        print(message)
                        return
                if not obj.print_errors:
                    raise exc
                print(exc)
            except BaseException as exc:
                if not obj.print_errors:
                    raise exc
                print(exc)
    return wrapped
