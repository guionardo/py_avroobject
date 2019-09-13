import io
import json
import os
import re
import tempfile

import avro
import avro.schema
import avro.datafile
import avro.io
import avro_json_serializer

from .avro_tools import fetch_json, is_avro_binary, parse_schema


class AvroObject:
    """
    Helper class for AVRO objects

    ...


    Attributes
    ----------
    schema : avro.schema.Recordschema
    namespace : str

    data : object

    """

    def __init__(self, data=None, schema=None):
        self.namespace = None       # Schema Namespace
        self.original_data = data   # Original data from initialization
        self.object_data = None     # Deserialized data
        self.data = None
        self.type = None
        self.name = None
        self.origin = None
        self.ok = False
        self.realdata = data
        self.bin_data = None
        self.json_data = None
        self._last_error = None

        if isinstance(data, str):
            # Verifica se é um JSON
            dt = fetch_json(data)
            if dt[0]:
                try:
                    data = json.loads(dt[1])
                    self.object_data = data
                except Exception as e:
                    self._last_error = f"Erro em AvroObject({dt[0]}):{e}"
        elif is_avro_binary(data):
            if self._parsebytes(data)[0]:
                self.namespace = self.schema.namespace
                self.ExportToJSON()
                return
            else:
                data = None

        self.schema = parse_schema(schema)
        if self.schema:
            self.namespace = self.schema.namespace

        if data is not None:
            self.realdata = data
            self.ExportToBin()
            self.ExportToJSON()

    @property
    def LastError(self):
        return self._last_error

    def __str__(self):
        return f"AvroObject({self.name}:{'OK' if self.ok else 'ERROR'}) = {self.realdata}"

    def getSchemaInfos(self):
        """
        Retorna dict com informações sobre o último schema utilizado
        """
        return {
            "namespace": self.namespace,
            "type": self.type,
            "name": self.name,
            "origin": self.origin
        }

    def Parse(self, data, schema=None) -> tuple:
        """
        Trata informações e gera o objeto
        A tupla de retorno tem dois valores (Sucesso: bool, Mensagem: str)
        """
        if type(data) is bytes:
            return self._parsebytes(data)
        elif type(data) is str:
            return self._parsestr(data, schema)
        self.ok = False
        return False, 'Paramêtro inválido'

    def ExportToJSON(self) -> str:
        '''
        Exports the object to JSON string

        :return: str or None
        '''
        self._last_error = None
        if self.json_data is None:
            try:
                if self.object_data is None:
                    self._last_error = 'ExportToJSON: data is None'
                    return None

                if isinstance(self.schema, avro.schema.RecordSchema):
                    serializer = avro_json_serializer.AvroJsonSerializer(
                        self.schema)
                    self.json_data = serializer.to_json(self.object_data)
                else:
                    self.json_data = json.dumps(self.object_data)

            except Exception as e:
                self._last_error = f'ExportToJSON:{e}'

        return self.json_data

    def ExportToText_(self, data, schema=None) -> tuple:
        """
        Exporta objeto data utilizando o schema informado em formato texto (JSON)
        """
        if schema is not None:
            if self._parseschema(schema):
                schema = self.schema
        else:
            schema = self.schema

        try:
            if schema is None:
                self.json_data = json.dumps(data)
            else:
                serializer = avro_json_serializer.AvroJsonSerializer(schema)
                self.json_data = serializer.to_json(data)

            return True, self.json_data, self.getSchemaInfos()
        except Exception as e:
            return False, str(e), self.getSchemaInfos()

    def ExportToBin(self) -> bytes:
        '''
        Exports the object to binary AVRO format

        :return: bytes or None
        '''
        if self.realdata is None:
            self._last_error = 'ExportToBin: data is None'
            return None
        if not isinstance(self.schema, avro.schema.RecordSchema):
            self._last_error = 'ExportToBin: schema undefined'
            return None

        self._last_error = None

        if self.bin_data is not None:
            return self.bin_data

        try:
            with tempfile.SpooledTemporaryFile(suffix='.avro') as tmp:
                writer = avro.datafile.DataFileWriter(
                    tmp, avro.io.DatumWriter(), self.schema)
                for d in self.realdata if self.realdata is list else [self.realdata]:
                    writer.append(d)
                writer.flush()
                tmp.seek(0)
                self.bin_data = tmp.read()
                self.ok = True
                writer.close()
                tmp.close()
                return self.bin_data
        except Exception as e:
            self._last_error = f'ExportToBin:{e}'
            return None

    def ExportToBin_(self, data=None, schema=None) -> tuple:
        """
        Exporta objeto data utilizando o schema informado em formato binário (bytes)
        """
        if data is None:
            if self.realdata is None:
                return False, "Empty data"
            data = self.realdata

        if schema is not None:
            pschema = self._parseschema(schema)
            if pschema[0]:
                schema = self.schema
            else:
                return pschema

        else:
            schema = self.schema

        if not type(schema) is avro.schema.RecordSchema:
            schema = None
        try:
            with tempfile.SpooledTemporaryFile(suffix='.avro') as tmp:
                writer = avro.datafile.DataFileWriter(
                    tmp, avro.io.DatumWriter(), schema)
                if data is not list:
                    writer.append(data)
                else:
                    for d in data:
                        writer.append(d)
                writer.flush()
                tmp.seek(0)
                self.bin_data = tmp.read()
                self.ok = True
                writer.close()
                tmp.close()
            return True, self.bin_data, self.getSchemaInfos()
        except Exception as e:
            return False, str(e), self.getSchemaInfos()

    def _parsebytes(self, data) -> tuple:
        """Trata informações a partir de dados bytes"""
        try:
            bdata = io.BytesIO(data)
            reader = avro.datafile.DataFileReader(bdata, avro.io.DatumReader())
            cschema = reader.GetMeta('avro.schema')
            obj_data = []
            for datum in reader:
                obj_data.append(datum)

            if len(obj_data) == 1:
                obj_data = obj_data[0]

            reader.close()
            self.schema = avro.schema.Parse(cschema)
            self.object_data = obj_data
            self.origin = ('binary', None)
            self.ok = True
            return True, 'OK'
        except Exception as e:
            self.ok = False
            return False, str(e)

    def _parsestr(self, data, schema) -> tuple:
        '''Trata informações a partir de uma string json'''
        f = fetch_json(data)
        if not f[0]:  # Erro no fetch da informação
            return False, f[1]

        data = f[1]
        try:
            if schema is not None:
                self._parseschema(schema)

            if type(self.schema) is avro.schema.RecordSchema:
                deserializer = avro_json_serializer.AvroJsonDeserializer(
                    self.schema)
                obj = deserializer.from_json(data)
            else:
                obj = json.loads(data)

            self.data = obj
            self.origin = ('text', f[2])
            self.ok = True
            return True, 'OK'
        except Exception as e:
            self.ok = False
            return False, str(e)

    def _parsefile(self, data, schema) -> tuple:
        """Trata informações lidas a partir de um arquivo"""

        # Detectar se é um arquivo binário ou um texto com JSON
        try:
            binary = False
            filename = os.path.abspath(data)
            with open(filename, 'rb') as f:
                if f.read(3) == b'Obj':
                    # Arquivo binário
                    f.seek(0)
                    data = f.read()
                    binary = True
                f.close()
            if not binary:
                with open(data, 'r') as f:
                    data = f.read()
                    f.close()
            if binary:
                ret = self._parsebytes(data)
                self.origin = ('file/binary:', filename)
            else:
                ret = self._parsestr(data, schema)
                self.origin = ('file/text:', filename)
            return ret

        except Exception as e:
            return False, str(e)

    def _parseschema(self, schema) -> tuple:
        """Carrega a informação de um schema

        Argumento pode ser um objeto RecordSchema, um dict, um JSON, ou um arquivo com o conteúdo JSON, ou uma URL
        com o conteúdo JSON """

        if type(schema) is dict:
            schema = json.dumps(schema)

        if type(schema) is str:
            try:
                fetch = fetch_json(schema)
                if fetch[0]:
                    schema = fetch[1]
                else:
                    return False, fetch[1]

                schema = avro.schema.Parse(schema)
            except Exception as e:
                return False, str(e)

        if type(schema) is avro.schema.RecordSchema:
            self.schema = schema
            return True, 'OK'

        return False, 'NO SCHEMA'

    @staticmethod
    def isAvroBinary(bin_data: bytes) -> bool:
        """
        Identifica se uma sequencia de bytes é um binário AVRO válido
        """
        return is_avro_binary(bin_data)
