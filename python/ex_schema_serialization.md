# Schema Serialization

``` python
from avro_object import AvroObject
from datetime import datetime, date, time
from pprint import pprint

print("Schema Serialization")

schema = '''
{
    "doc": "Test schema",
    "name": "test_schema",
    "namespace": "test",
    "type": "record",
    "fields": [
        {
            "name": "id",
            "type": "int"
        },
        {
            "name": "name",
            "type": "string"
        }
    ]
}'''

schemas = [{
    'doc': 'A weather reading.',
    'name': 'Weather',
    'namespace': 'test',
    'type': 'record',
    'fields': [
            {'name': 'station', 'type': 'string'},
            {'name': 'time', 'type': 'long'},
            {'name': 'temp', 'type': 'int'},
    ],
}, schema,
    "examples/schema.json"
]

records = [
    {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
    {'id': 1, 'name': 'Guionardo'},
    {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
]

print("Schema")

for i in range(0, 3):
    schema = schemas[i]
    record = records[i]
    ao = AvroObject(record, schema)
    print(f'\n#{i}: Schema [{ao.schema_origin}] = {str(schema)[0:40]}')
    print(f'\nJSON')
    pprint(ao.to_json())
    print(f'\nAVRO Binary')
    pprint(ao.to_avro())
    print('\nObject')
    pprint(ao.data)
```

## OUTPUT

``` shell
Schema Serialization
Schema

#0: Schema [dict] = {'doc': 'A weather reading.', 'name': 'W

JSON
'{"station": "011990-99999", "time": 1433269388, "temp": 0}'

AVRO Binary
(b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xc0\x02{"type": "record"'
 b', "name": "test.Weather", "fields": [{"name": "station", "type": "string"}, '
 b'{"name": "time", "type": "long"}, {"name": "temp", "type": "int"}]}\x00'
 b'\xd8\x19\xba/Z\x18\xd2(\xba\xbc2\xa2\xa5\xe5dk\x02&\x18011990-99999\x98'
 b'\xd2\xef\xd6\n\x00\xd8\x19\xba/Z\x18\xd2(\xba\xbc2\xa2\xa5\xe5dk')

Object
{'station': '011990-99999', 'temp': 0, 'time': 1433269388}

#1: Schema [string] =
{
    "doc": "Test schema",
    "name":

JSON
'{"id": 1, "name": "Guionardo"}'

AVRO Binary
(b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xfa\x01{"type": "record"'
 b', "name": "test.test_schema", "fields": [{"name": "id", "type": "int"}, {"na'
 b'me": "name", "type": "string"}]}\x00\xfd\xc3\xd1\xa8\xac\xb8\xa5\x04z\xc5\t'
 b'\x9d}\x87\xd8\xe5\x02\x16\x02\x12Guionardo\xfd\xc3\xd1\xa8\xac\xb8'
 b'\xa5\x04z\xc5\t\x9d}\x87\xd8\xe5')

Object
{'id': 1, 'name': 'Guionardo'}

#2: Schema [file://examples/schema.json] = examples/schema.json

JSON
'{"station": "012650-99999", "time": 1433275478, "temp": 111}'

AVRO Binary
(b'Obj\x01\x04\x14avro.codec\x08null\x16avro.schema\xc0\x02{"type": "record"'
 b', "name": "test.Weather", "fields": [{"name": "station", "type": "string"}, '
 b'{"name": "time", "type": "long"}, {"name": "temp", "type": "int"}]}\x00'
 b'\xb6\xf6\xd9\x8e\xf7v\xb30\x03)\x08\xa7=\x9e$\xb9\x02(\x18012650-99999\xac'
 b'\xb1\xf0\xd6\n\xde\x01\xb6\xf6\xd9\x8e\xf7v\xb30\x03)\x08\xa7=\x9e$\xb9')

Object
{'station': '012650-99999', 'temp': 111, 'time': 1433275478}
```
