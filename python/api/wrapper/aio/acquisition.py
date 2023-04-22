"""
The module contains classes and supporting methods
for operations with Acquisition endpoints.
"""

from abc import ABC
from pathlib import Path
from httpx import Response

import json

from .generic import _GenericIntegrator
from .utils import _read_endpoints, _read_token, _rectify_default_kwargs

_base_url, _endpoints = _read_endpoints('acquisition')


def _compile_file_details(*,
                          feed: str = None,
                          tenant: str = None,
                          system: str = None,
                          vendor: str = None,
                          file_details: dict = None,
                          **kwargs):
    if not file_details:
        file_details = {
            "tenantId": tenant,
            "vendor": vendor,
            "system": system,
            "feed": feed
        }
        file_details.update(kwargs)
    return file_details


class _GenericAcquisition(_GenericIntegrator, ABC):
    """
    A base class for operating with Acquisition endpoints.
    The class consists of call-functions to Acquisition endpoints.
    It should be overridden with a class extracting the json objects
    from the responses. All methods accept parameters that are used
    to format the endpoint url or passed along with the request.
    """

    def _compile_headers(self):
        return {
            'accept': 'application/json, text/plain, */*',
            'Authorization': fr'Bearer {self._token}',
            'accept-encoding': 'gzip, deflate, br'
        }

    async def _upload_file(self, file, *,
                           file_details: dict = None,
                           **kwargs) -> Response:
        params = {
            'fileDetails': json.dumps(file_details)
        }
        with open(file, 'rb') as f:
            files = {'file': (file.name, f)}
            return await self._call('file_upload',
                                    files=files,
                                    params=params,
                                    **kwargs)


class _AcquisitionExtractor(_GenericAcquisition, ABC):
    """
    A base class for operating with Acquisition endpoints.
    The class converts Response objects returned by the
    parent class's methods to dict | list objects.
    """

    async def upload_file(self, file, *,
                          vendor: str = None,
                          system: str = None,
                          feed: str = None,
                          file_details: dict = None,
                          **kwargs) -> dict:
        tenant = self.tenant
        fd_kwargs = locals().copy()
        del fd_kwargs['file']
        _rectify_default_kwargs(fd_kwargs, **kwargs)

        file_details = _compile_file_details(**fd_kwargs)

        return self._extract_data(
            await self._upload_file(
                file,
                file_details=file_details))


class Acquisition(_AcquisitionExtractor):
    """
    A class for initiating the Acquisition object.
    The Acquisition class is used for making API requests
    to Acquisition endpoints.
    """

    def __init__(self, env: str, tenant: str, *,
                 token: str = None,
                 token_path: str | Path = None,
                 print_errors: bool = False,
                 return_meta_data: bool = False,
                 **kwargs):
        token = token or _read_token(token_path=token_path)
        super().__init__(env, tenant,
                         token=token,
                         endpoints=_endpoints,
                         base_url=_base_url,
                         print_errors=print_errors,
                         return_meta_data=return_meta_data,
                         **kwargs)
