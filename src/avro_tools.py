import json
import os
import re

import avro
import avro.schema
import requests


def fetch_json(source: str) -> tuple:
    '''Carrega a informação JSON de várias mídias e devolve como string

    :param source: string JSON, nome de arquivo, URL
    :return tuple: (Sucesso, conteúdo ou mensagem de erro, origem)
    '''
    try:
        # Verifica se é um arquivo existente
        if os.path.exists(source) and os.path.isfile(source):
            with open(source, 'r') as f:
                content = f.read()
                f.close()
                return True, content, os.path.abspath(source)

        # Verifica se é uma URL
        pattern = re.compile(
            r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+")
        if pattern.fullmatch(source):
            content = requests.get(url=source).text
            return True, content, source

        # Tenta fazer o parse do JSON (json.loads estoura caso o source seja inválido)
        json.loads(source)
        return True, source, "string"
    except Exception as e:
        return True, str(e), None


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
