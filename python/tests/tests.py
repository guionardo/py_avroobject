import unittest
from pprint import pprint
import os

from avro_object import AvroObject, AvroTools


class AvroObjectTests(unittest.TestCase):

    def setUp(self):
        self.schema = {
            'namespace': 'avroobject.test',
            'type': 'record',
            'name': 'User',
            'fields': [
                {'name': 'UserName', 'type': 'string'},
                {'name': 'Age', 'type': ['int', 'null'], 'default':'1'},
                {'name': 'Active', 'type': 'boolean', 'default': 'False'}
            ]
        }
        self.obj_ok = {'UserName': 'Guionardo',
                       'Age': 42,
                       'Active': True}
        self.obj_err = {'UserName': 'Guionardo',
                        'Age': '42'
                        }

        self.serial_bin = b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xd6\x03{"type": "record"'\
            b', "name": "User", "namespace": "avroobject.test", "fields": [{"type": "strin'\
            b'g", "name": "UserName"}, {"type": ["int", "null"], "name": "Age", "default":'\
            b' "1"}, {"type": "boolean", "name": "Active", "default": "False"}]}\x00\xe0'\
            b'\xa4\x92D\xe5\x84y\x89\x0e0z\xce\xc9\xa1\x18\xe5\x02\x1a\x12Guionardo\x00'\
            b'T\x01\xe0\xa4\x92D\xe5\x84y\x89\x0e0z\xce\xc9\xa1\x18\xe5'

        self.serial_str = '{"UserName":"Guionardo","Age":{"int":42},"Active":true}'

    def test_add_parser(self):
        def f_ok(source: str) -> tuple:
            return True, "Valid function"

        def f_not_ok1(source: str, extra_arg: int) -> tuple:
            return False, "Invalid function 1"

        def f_not_ok2() -> tuple:
            return False, "Invalid function 2"

        self.assertTrue(AvroTools.add_fetch_method(f_ok))
        self.assertFalse(AvroTools.add_fetch_method(f_not_ok1))
        self.assertFalse(AvroTools.add_fetch_method(f_not_ok2))

        AvroTools.reset_fetch_methods()

    def test_schema(self):

        o = {
            'nome': 'teste',
            'idade': 32,
            'ativo': True
        }

        s = AvroTools.create_schema(o, 'schema_teste')
        pprint(s)
        pprint(o)
        self.assertTrue(True, 'ok')

    def test_objet_of_schema(self):
        ao = AvroObject(self.obj_ok, self.schema)
        self.assertTrue(ao.ok)

    def test_invalid_object_of_schema(self):
        ao = AvroObject(self.obj_err, self.schema)
        self.assertFalse(ao.ok)

    def test_loaded_object_schema(self):
        file_object = os.path.realpath('./examples/delivery.json')
        file_schema = os.path.realpath('./examples/delivery.avsc')

        ao = AvroObject(file_object, file_schema)
        self.assertTrue(ao.ok, ao.last_error)

    def test_loaded_bad_object_good_schema(self):
        file_object = os.path.realpath('./examples/delivery_bad.json')
        file_schema = os.path.realpath('./examples/delivery.avsc')
        ao = AvroObject(file_object, file_schema)
        self.assertFalse(ao.ok, ao.last_error)

    def test_validateSchema(self):
        self.assertTrue(AvroTools.validateSchema(self.schema))
        badSchema = {'anydata': False}
        self.assertFalse(AvroTools.validateSchema(badSchema))

    def test_serialize_str(self):
        ao = AvroObject(self.obj_ok, self.schema)
        serial_json = ao.to_json()
        self.assertIsNone(ao.last_error)

    def test_serialize_bin(self):
        ao = AvroObject(self.obj_ok, self.schema)
        serial_bin = ao.to_avro()
        self.assertIsNone(ao.last_error)
        pprint(self.serial_bin)
        pprint(serial_bin)

    def test_serialize_bin2(self):
        ao = AvroObject(self.obj_err, self.schema)
        self.assertIsNotNone(ao.last_error)

    def test_serialize(self):
        ao = AvroObject(self.obj_ok, self.schema)
        self.assertIsNone(ao.last_error)
        pprint(ao.to_json())
        pprint(ao.to_avro())

    def test_deserialize_json(self):
        obj_json = '{"UserName":"Guionardo","Age":42,"Active":true}'
        aoj = AvroObject(obj_json)
        if aoj.last_error:
            print(aoj.last_error)
        self.assertIsNone(aoj.last_error)

    def test_deserialize_avro(self):
        obj_avro = (b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xd6\x03{"type": "record"'
                    b', "name": "User", "namespace": "avroobject.test", "fields": [{"type": "strin'
                    b'g", "name": "UserName"}, {"type": ["int", "null"], "name": "Age", "default":'
                    b' "1"}, {"type": "boolean", "name": "Active", "default": "False"}]}\x00\xe0'
                    b'\xa4\x92D\xe5\x84y\x89\x0e0z\xce\xc9\xa1\x18\xe5\x02\x1a\x12Guionardo\x00'
                    b'T\x01\xe0\xa4\x92D\xe5\x84y\x89\x0e0z\xce\xc9\xa1\x18\xe5')
        aob = AvroObject(obj_avro)
        pprint(aob.last_error)


if __name__ == '__main__':
    unittest.main()
