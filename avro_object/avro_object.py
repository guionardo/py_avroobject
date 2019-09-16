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

    def __init__(self, data, schema=None):
        """
        :param data: dict, list of dicts, JSON str, file
        """
        self.namespace = None       # Schema Namespace
        self.original_data = data   # Original data from initialization
        self.object_data = None     # Deserialized data
        self.bin_data = None        # AVRO binary serializaded data
        self.json_data = None       # JSON string serializaded data
        self.schema = parse_schema(schema)  # AVRO schema

        self.data = None
        self.type = None
        self.name = None        
        self.ok = False
        self.realdata = data
        self._last_error = None

        if isinstance(data, str):
            # JSON string
            dt = fetch_json(data)
            if not dt[0]:
                self._last_error = dt[1]
                return

            try:
                self.object_data = json.loads(dt[1])
                if self.schema is None:
                    self.ok = True
                else:
                    self._parsestr(self.data)

            except Exception as e:
                self._last_error = f"AvroObject error({dt[0]}):{e}"

        elif is_avro_binary(data):
            # AVRO binary
            if self._parsebytes(data)[0]:
                self.namespace = self.schema.namespace
                self.ok = self.ExportToJSON() is not None

            else:
                data = None

        elif isinstance(data, dict) or isinstance(data, list):
            # Dict or List
            self.object_data = data
            self.ok = True
            if self.schema is not None:
                self.ok = self.ExportToBin() is not None

        else:
            self._last_error = 'Invalid data'

        if self.schema:
            self.namespace = self.schema.namespace

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
            "name": self.name            
        }

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
            self.ok = True
            return True, 'OK'
        except Exception as e:
            self.ok = False
            return False, str(e)

    def _parsestr(self, data) -> tuple:
        """Parses JSON string using schema

        :param data: str JSON
        :return: tuple (bool Success, str Message) 
        """
        success, info = fetch_json(data)
        if not success:  # Erro no fetch da informação
            return False, info

        data = info
        try:
            if isinstance(self.schema, avro.schema.RecordSchema):
                deserializer = avro_json_serializer.AvroJsonDeserializer(
                    self.schema)
                obj = deserializer.from_json(data)
            else:
                obj = json.loads(data)

            self.data = obj            
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
            else:
                ret = self._parsestr(data)                
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
