"""
The module contains base classes and supporting methods
for operations with API endpoints.
"""

from abc import ABC
from contextlib import suppress
from pathlib import Path

import requests

from .utils import raise_for_status, _read_token


class _GenericController(ABC):
    """
    A base class for operating with API endpoints.
    The class stores session details, such as
    the environment and token, provides call functions
    and validates them for errors.
    """

    def __init__(self, env: str, tenant: str, *,
                 token: str,
                 organization: str = None,
                 custom_headers: dict = None):
        self.env = env.lower()
        self.tenant = tenant.lower() if tenant else None
        self.organization = organization

        self._token = token
        self._headers = self._compile_headers(custom_headers)

    def _compile_headers(self, custom_headers: dict):
        headers = {
            'accept': '*/*',
            'Authorization': fr'Bearer {self._token}',
            'Content-Type': "application/json",
            'X-context': self.tenant
        }
        if custom_headers:
            headers.update(custom_headers)
        return headers

    @raise_for_status
    def _get(self, url: str,
             **kwargs) -> requests.Response:
        headers = kwargs.pop('headers', self._headers)
        return requests.get(url,
                            headers=headers,
                            **kwargs)

    @raise_for_status
    def _post(self, url: str,
              **kwargs) -> requests.Response:
        headers = kwargs.pop('headers', self._headers)
        return requests.post(url,
                             headers=headers,
                             **kwargs)

    @raise_for_status
    def _put(self, url: str,
             **kwargs) -> requests.Response:
        headers = kwargs.pop('headers', self._headers)
        return requests.put(url,
                            headers=headers,
                            **kwargs)

    @raise_for_status
    def _patch(self, url: str,
               **kwargs) -> requests.Response:
        headers = kwargs.pop('headers', self._headers)
        return requests.patch(url,
                              headers=headers,
                              **kwargs)

    @raise_for_status
    def _delete(self, url: str,
                **kwargs) -> requests.Response:
        headers = kwargs.pop('headers', self._headers)
        return requests.delete(url,
                               headers=headers,
                               **kwargs)


class _GenericIntegrator(_GenericController, ABC):
    """
    A base class for operating with API endpoints.
    The class parses url endpoints, executes calls
    and returns Response objects.
    """

    def __init__(self, *args,
                 endpoints: dict,
                 base_url: str,
                 print_errors: bool = False,
                 return_meta_data: bool = False,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.print_errors = print_errors
        self.return_meta_data = return_meta_data
        self._base_url = base_url
        self._endpoints = endpoints
        self._methods = self._compile_methods()

    def _compile_methods(self) -> dict:
        return {
            'get': self._get,
            'post': self._post,
            'put': self._put,
            'patch': self._patch,
            'delete': self._delete
        }

    def _extract_data(self, response, data_key: str = None):
        if response:
            try:
                response_data = response.json()
            except BaseException as exc:
                if self.print_errors:
                    print(exc)
                return response.text
            if not self.return_meta_data and data_key:
                with suppress(KeyError):
                    response_data = response_data[data_key]
            return response_data
        elif not self.print_errors:
            raise ValueError('got NoneType object as a response')

    def _call(self, key: str, *,
              url_format_params: dict = None,
              **kwargs) -> requests.Response:
        # determining the call method and
        # formatting the endpoint url
        method, url = self._get_call_params(key,
                                            url_format_params)

        # executing the request and returning results
        return method(url,
                      **kwargs)

    def _get_call_params(self, key: str,
                         url_format_params: dict = None) -> tuple:
        # preparing local variables
        url_format_params = url_format_params or {}
        endpoint = self._endpoints[key]
        url_template = endpoint['path']
        method_name = endpoint['method'].lower()

        # determining the call method
        method = self._methods[method_name]

        # formatting the url to call
        url = self._parse_url(url_template,
                              **url_format_params)

        return method, url

    def _parse_url(self, url_template: str, *args, **kwargs) -> str:
        # preparing local variables
        url = self._base_url + url_template
        env = kwargs.pop('env', self.env)

        # formatting the url
        return url.format(*args,
                          env=env,
                          **kwargs)


class SampleApiModule(_GenericController):
    """
    A class for initiating a module object.
    The class is used for generic API
    requests to a module's endpoints.
    """

    def __init__(self, env: str, tenant: str, *,
                 token: str = None,
                 token_path: str | Path = None,
                 **kwargs):
        token = token or _read_token(token_path=token_path)
        super().__init__(env, tenant,
                         token=token,
                         **kwargs)

    def get(self, *args, **kwargs) -> requests.Response:
        return self._get(*args, **kwargs)

    def post(self, *args, **kwargs) -> requests.Response:
        return self._post(*args, **kwargs)

    def put(self, *args, **kwargs) -> requests.Response:
        return self._put(*args, **kwargs)

    def patch(self, *args, **kwargs) -> requests.Response:
        return self._patch(*args, **kwargs)

    def delete(self, *args, **kwargs) -> requests.Response:
        return self._delete(*args, **kwargs)
