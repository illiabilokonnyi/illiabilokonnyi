from collections import deque
from contextlib import suppress
from functools import reduce

import operator


def _recursive_extract(lookup_path,
                       data,
                       **kwargs):
    recursive = _RecursiveExtractor(lookup_path,
                                    data,
                                    **kwargs)
    extracted = recursive.extract()
    return extracted


class _RecursiveExtractor:
    def __init__(self,
                 lookup_path: deque,
                 data: dict | list,
                 record_path: list = None,
                 primary_key_paths: dict = None,
                 parent=None):
        self.lookup_path = lookup_path.copy()
        self.record_path = record_path or ''
        self.data = data
        self.records = []
        self.primary_keys = parent.primary_keys.copy() if parent else {}
        self.primary_key_paths = primary_key_paths.copy() if primary_key_paths \
            else parent.primary_key_paths.copy() if parent \
            else {}
        self.parent = parent
        self._type_methods = {
            list: self._extract_list,
            dict: self._extract_dict
        }

    def extract(self) -> list:
        if self.lookup_path:
            for data_type, extraction_method in self._type_methods.items():
                if isinstance(self.data, data_type):
                    extraction_method()
                    return self.records
        self._extract_record()
        return self.records

    def _extract_list(self):
        for record in self.data:
            self._extract_child_record(record, self.record_path)

    def _extract_dict(self):
        self._search_primary_keys()
        key = self.lookup_path.popleft()
        record = self.data[key]
        record_path = '/'.join([self.record_path, key]) if self.record_path else key
        self._extract_child_record(record, record_path)

    def _extract_record(self):
        record = self.data.copy()
        if isinstance(record, list):
            for sub_record in record:
                sub_record.update(self.primary_keys)
        elif isinstance(record, dict):
            record.update(self.primary_keys)
            record = [record]
        elif record is not None:
            raise TypeError(f'Unsupported type [{type(record)}]')
        self.records.extend(record)

    def _extract_child_record(self, record, record_path):
        extracted = _recursive_extract(self.lookup_path,
                                       record,
                                       record_path=record_path,
                                       parent=self
                                       )
        self.records.extend(extracted)

    def _search_primary_keys(self):
        for key, path in tuple(self.primary_key_paths.items()):
            if self.record_path:
                path = path.removeprefix(f'{self.record_path}/')
            path = path.split('/')
            if path[0] in self.data:
                with suppress(BaseException):
                    value = reduce(operator.getitem,
                                   path,
                                   self.data)
                    self.primary_keys[key] = value
                    del self.primary_key_paths[key]


class ModelExtractor:
    def __init__(self,
                 path_base: str,
                 model_path: str,
                 added_attributes_paths: dict = None) -> None:
        self.path_base = deque(path_base.split('/'))
        self.model_path = deque(model_path.split('/'))
        self.added_attributes_paths = added_attributes_paths

    def extract(self, data: dict | list):
        base_data = reduce(operator.getitem,
                           self.path_base,
                           data)
        return _recursive_extract(self.model_path,
                                  base_data,
                                  primary_key_paths=self.added_attributes_paths)
