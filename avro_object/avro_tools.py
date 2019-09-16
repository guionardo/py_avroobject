import json
import os
import re

import avro
import avro.schema
import requests

from inspect import signature

_json_fetch_modes = []


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


def is_avro_binary(bin_data: bytes) -> bool:
    """
    Identifica se uma sequencia de bytes é um binário AVRO válido
    """

    return isinstance(bin_data, bytes) and (len(bin_data) > 15) and (bin_data[:16] == b'Obj\x01\x04\x14avro.codec')


def parse_schema(schema) -> avro.schema.RecordSchema:
    '''
    Validate and process schema

    :param schema: RecordSchema object, dict, JSON string, file with JSON, URL with JSON content
    :return: RecordSchema or None
    '''
    if isinstance(schema, dict):
        schema = json.dumps(schema)

    if isinstance(schema, str):
        try:
            fetch = fetch_json(schema)
            if fetch[0]:
                schema = avro.schema.Parse(fetch[1])

        except Exception as e:
            print(e)
            schema = None

    if isinstance(schema, avro.schema.RecordSchema):
        return schema

    return None


for method in [fetch_json_file, fetch_json_url]:
    _json_fetch_modes.append(method)
