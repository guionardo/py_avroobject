import io
import json
import numbers
import os
import re
import tempfile
from inspect import signature
from pprint import pprint

import requests
from fastavro import (is_avro, json_writer, parse_schema, reader, validate,
                      writer)

_json_fetch_modes = []


class AvroObject:
    """
    Helper class for AVRO objects

    ...

    Properties
    ----------
    data: 
    origin:
    ok:
    last_error:

    Methods
    -------
    to_avro()
    to_json()
    """

    def __init__(self, data, schema=None):
        """
        :param data: dict, list of dicts, JSON str, file, bytes
        :param schema: dict
        """
        self._last_error = None
        self._object_data = None
        self._json_data = None
        self._avro_data = None
        self._origin = None

        self._ok = False
        self._schema = None if schema is None else parse_schema(schema)

        if isinstance(data, bytes):
            b_avro = False
            try:
                bdata = io.BytesIO(data)
                if is_avro(bdata):
                    self._origin = 'binary_avro'
                    bdata.seek(0)
                    b_avro = True
                    avro_reader = reader(bdata)
                    self._schema = avro_reader.schema
                    obj_data = []
                    for record in avro_reader:
                        obj_data.append(record)
                    self._object_data = None if len(obj_data) == 0 else obj_data[0] if len(
                        obj_data) == 1 else obj_data
                    self._ok = True
                else:
                    self._origin = 'binary_string'
                    data = data.decode('utf-8')

            except Exception as e:
                self._last_error = (
                    'Avro binary' if b_avro else 'String decoding')+f' error: {e}'

        if isinstance(data, str):
            success, json_data, origin = fetch_json(data)
            if not self._origin:
                self._origin = origin
            if not success:
                self._last_error = json_data
                return

            try:
                self._object_data = json.loads(json_data)
                self._json_data = json_data
                if self._schema is None:
                    self._ok = True
            except Exception as e:
                self._last_error = f'JSON parsing error: {e}'

        elif isinstance(data, dict) or isinstance(data, list):
            self._origin = type(data).__name__
            self._object_data = data
            if self._schema is None:
                self._ok = True

        if self._object_data is not None and not self._ok and self._schema is not None:
            try:
                validate(self._object_data, self._schema)
                self._ok = True
            except Exception as e:
                self._last_error = f'Schema error: {e}'

    def __str__(self):
        return f'{self._origin}:{self.to_json()}'

    def to_json(self):
        if not self._ok or self._json_data:
            return self._json_data

        if self._schema is None:
            self._json_data = json.dumps(self._object_data)
        else:
            out = io.StringIO()
            json_writer(out, self._schema, self._object_data if isinstance(
                self._object_data, list) else [self._object_data])
            out.flush()
            out.seek(0)
            self._json_data = out.read()

        return self._json_data

    def to_avro(self):
        if not self._ok or self._avro_data or not self._schema:
            return self._avro_data

        out = io.BytesIO()
        writer(out, self._schema, self._object_data if isinstance(
            self._object_data, list) else [self._object_data])
        out.flush()
        out.seek(0)
        self._avro_data = out.read()
        return self._avro_data

    @property
    def data(self):
        return self._object_data

    @property
    def origin(self):
        return self._origin

    @property
    def ok(self):
        return self._ok

    @property
    def last_error(self):
        return self._last_error


def create_schema(data: dict, name: str, namespace: str = 'namespace.test', doc: str = None):
    '''
    Create schema from object
    '''
    if not isinstance(data, dict):
        return None
    schema = {
        'doc': 'A weather reading.',
        'name': 'Weather',
        'namespace': 'test',
        'type': 'record',
        'fields': [
            {'name': 'station', 'type': 'string'},
            {'name': 'time', 'type': 'long'},
            {'name': 'temp', 'type': 'int'},
        ],
    }

    o_type = None
    if isinstance(data, dict):
        o_type = 'record'
    elif isinstance(data, list):
        o_type = 'array'
    elif isinstance(data, str):
        o_type = 'string'
    elif isinstance(data, numbers.Integral):
        o_type = 'int'
    elif isinstance(data, numbers.Real):
        o_type = 'float'
    else:
        return None
    if not name:
        return None
    schema = {
        'namespace': namespace,
        'type': o_type,
        'name': name,
        'fields': []
    }
    if isinstance(doc, str) and len(doc) > 0:
        schema['doc'] = doc

    for k, v in data.items():
        field = create_schema(data=v, name=k)
        schema['fields'].append(field)

    return schema    


def add_fetch_method(method) -> bool:
    try:
        sign = signature(method)
        if len(sign.parameters) != 1:
            return False
        if not (sign.return_annotation is tuple):
            return False
        if method not in _json_fetch_modes:
            _json_fetch_modes.append(method)
        return True
    except:
        pass
    return False


def reset_fetch_methods():
    _json_fetch_modes.clear()
    for method in [fetch_json_file, fetch_json_url]:
        _json_fetch_modes.append(method)


def fetch_json(source: str) -> tuple:
    '''Load JSON string from various medium and returns as string

    :param source: string JSON, file name, URL, another registered source by add_fetch_method
    :return tuple: (bool Success, str JSON or error message, origin)
    '''
    try:
        for method in _json_fetch_modes:
            success, message = method(source)
            if success:
                break
        # Try to parse JSON str
        json.loads(message if success else source)
        return True, source, "string"
    except Exception as e:
        return False, str(e), None


def fetch_json_file(source: str) -> tuple:
    """Try to parse json from file

    :param source: str with file name
    :return: (bool Success, str JSON or Error)
    """
    if os.path.isfile(source):
        try:
            with open(source, 'r') as f:
                content = f.read()
            return True, content
        except Exception as e:
            return False, str(e)
    else:
        return False, f"File not found: {source}"


def fetch_json_url(source: str) -> tuple:
    """Try to parse json from url

    :param source: str with URL
    :return: (bool Success, str JSON or Error)
    """

    pattern = re.compile(
        r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+")
    if pattern.fullmatch(source):
        try:
            content = requests.get(url=source).text
            return True, content
        except Exception as e:
            return False, str(e)
    else:
        return False, f"Source is not an URL: {source}"


reset_fetch_methods()

if __name__ == "__main__":
    schema = {
        'doc': 'A weather reading.',
        'name': 'Weather',
        'namespace': 'test',
        'type': 'record',
        'fields': [
            {'name': 'station', 'type': 'string'},
            {'name': 'time', 'type': 'long'},
            {'name': 'temp', 'type': 'int'},
        ],
    }

    if True:
        records = [
            {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
            {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
            {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
            {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
        ]

    data = []
    data.append(records[0])
    data.append(b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xc0\x02{"type": "record"'
                b', "name": "test.Weather", "fields": [{"name": "station", "type": "string"}, '
                b'{"name": "time", "type": "long"}, {"name": "temp", "type": "int"}]}\x00'
                b'gv\x9d4Ul\xbb\xc2\x86\x8a\x91\x93\xb0/\x80S\x02&\x18011990-99999\x98'
                b'\xd2\xef\xd6\n\x00gv\x9d4Ul\xbb\xc2\x86\x8a\x91\x93\xb0/\x80S')
    data.append('{"station": "011990-99999", "time": 1433269388, "temp": 0}')

    for dt in data:
        ao = AvroObject(dt, schema)
        print()
        print(ao)
        pprint(ao.to_avro())
        pprint(ao.to_json())
