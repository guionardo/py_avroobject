# Example: Schema Serialization
#
# Just an dict to JSON
#
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
