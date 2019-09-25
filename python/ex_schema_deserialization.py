# Example: Schema Deserialization
#
# Just an dict to JSON
#
from avro_object import AvroObject
from datetime import datetime, date, time
from pprint import pprint

print("Schema Deserialization")

avros = [
    b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xc0\x02{"type": "record"'
    b', "name": "test.Weather", "fields": [{"name": "station", "type": "string"}, '
    b'{"name": "time", "type": "long"}, {"name": "temp", "type": "int"}]}\x00'
    b'\xef\x96\xba\xae\x16M\x15y|\xd9\x16"\x17\x9en\x91\x02(\x18012650-99999\xac'
    b'\xb1\xf0\xd6\n\xde\x01\xef\x96\xba\xae\x16M\x15y|\xd9\x16"\x17\x9en\x91',
    b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xc0\x02{"type": "record"'
    b', "name": "test.Weather", "fields": [{"name": "station", "type": "string"}, '
    b'{"name": "time", "type": "long"}, {"name": "temp", "type": "int"}]}\x00'
    b'\xef\x96\xba\xae\x16M\x15y|\xd9\x16"\x17\x9en\x91\x02(\x18012650-99999\xac'
    b'\xb1\xf0\xd6\n\xde\x01\xef\x96\xba\xae\x16M\x15y|\xd9\x16"\x17\x9en\x91',
    b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xfa\x01{"type": "record"'
    b', "name": "test.test_schema", "fields": [{"name": "id", "type": "int"}, {"na'
    b'me": "name", "type": "string"}]}\x00\xe9T?\x82\xa8\x80\x1deF\xb1E\x19n\xe5m'
    b'\n\x02\x16\x02\x12Guionardo\xe9T?\x82\xa8\x80\x1deF\xb1E\x19n\xe5m\n'
]

i = 0
for avro in avros:
    i += 1
    ao = AvroObject(avro)
    print(f'\n#{i}: Avro Object')
    print('\nJSON')
    pprint(ao.to_json())
    print('\nOBJECT')
    pprint(ao.data)
