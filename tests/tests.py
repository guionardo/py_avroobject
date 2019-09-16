import unittest
from pprint import pprint

from avro_object import AvroObject, create_schema, add_fetch_method


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

        self.serial_bin = b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xd6\x03{"type": "record"'
        b', "name": "User", "namespace": "avroobject.test", "fields": [{"type": "strin'
        b'g", "name": "UserName"}, {"type": ["int", "null"], "name": "Age", "default":'
        b' "1"}, {"type": "boolean", "name": "Active", "default": "False"}]}\x00\xe0'
        b'\xa4\x92D\xe5\x84y\x89\x0e0z\xce\xc9\xa1\x18\xe5\x02\x1a\x12Guionardo\x00'
        b'T\x01\xe0\xa4\x92D\xe5\x84y\x89\x0e0z\xce\xc9\xa1\x18\xe5'

        self.serial_str = '{"UserName":"Guionardo","Age":{"int":42},"Active":true}'

    def test_add_parser(self):
        def f_ok(source: str)->tuple:
            return True, "Valid function"
        def f_not_ok1(source:str, extra_arg: int)->tuple:
            return False, "Invalid function 1"
        def f_not_ok2()->tuple:
            return False, "Invalid function 2"

        self.assertTrue(add_fetch_method(f_ok))
        self.assertFalse(add_fetch_method(f_not_ok1))
        self.assertFalse(add_fetch_method(f_not_ok2))
        
    def test_schema(self):

        o = {
            'nome': 'teste',
            'idade': 32,
            'ativo': True
        }

        #s = create_schema(o, 'schema_teste')
        pprint(o)
        self.assertTrue(True, 'ok')

    def test_serialize_str(self):
        ao = AvroObject(self.obj_ok, self.schema)
        serial_json = ao.ExportToJSON()
        self.assertIsNone(ao.LastError)
        self.assertTrue(self.serial_str == serial_json)

    def test_serialize_bin(self):
        ao = AvroObject(self.obj_ok, self.schema)
        serial_bin = ao.ExportToBin()
        self.assertIsNone(ao.LastError)
        self.assertTrue(self.serial_bin == serial_bin)

    def test_serialize(self):
        ao = AvroObject(self.obj_ok, self.schema)
        self.assertIsNone(ao.LastError)
        pprint(ao.ExportToJSON())
        pprint(ao.ExportToBin())        

    def test_deserialize_json(self):
        obj_json = '{"UserName":"Guionardo","Age":42,"Active":true}'
        aoj = AvroObject(obj_json)
        pprint(aoj.LastError)
        self.assertIsNone(aoj.LastError)

    def test_deserialize_avro(self):
        obj_avro = (b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xd6\x03{"type": "record"'
                    b', "name": "User", "namespace": "avroobject.test", "fields": [{"type": "strin'
                    b'g", "name": "UserName"}, {"type": ["int", "null"], "name": "Age", "default":'
                    b' "1"}, {"type": "boolean", "name": "Active", "default": "False"}]}\x00\xe0'
                    b'\xa4\x92D\xe5\x84y\x89\x0e0z\xce\xc9\xa1\x18\xe5\x02\x1a\x12Guionardo\x00'
                    b'T\x01\xe0\xa4\x92D\xe5\x84y\x89\x0e0z\xce\xc9\xa1\x18\xe5')
        aob = AvroObject(obj_avro)
        pprint(aob.LastError)


if __name__ == '__main__':
    unittest.main()
