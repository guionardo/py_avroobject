# Basic Deserialization

 Just an dict to JSON

``` python
from avro_object import AvroObject
from datetime import datetime, date, time
from pprint import pprint

print("Basic Deserialization\n")

json = '''
{
    "id": 1,
    "name": "Guionardo",
    "birthdate": "1977-02-05",
    "today": "2019-09-24 21:16:48.671843",
    "now": "23:20:22",
    "loved_languages": [
        "Python",
        "C#"
    ],
    "good_guy": true,
    "weight_kg": 71.6,
    "height_m": 1.72
}'''

sources = [
    json,
    "examples/good_guy.json",
    "https://support.oneskyapp.com/hc/en-us/article_attachments/202761627/example_1.json"
]

i = 0
for source in sources:
    i+=1
    print(f"\n#{i}: JSON from {source}")
    ao = AvroObject(source)
    print("\nJSON content")
    print(ao.json)
    print("\nAvroObject.data (object deserialized)")
    pprint(ao.data)
```

## OUTPUT

```
Basic Deserialization


#1: JSON from
{
    "id": 1,
    "name": "Guionardo",
    "birthdate": "1977-02-05",
    "today": "2019-09-24 21:16:48.671843",
    "now": "23:20:22",
    "loved_languages": [
        "Python",
        "C#"
    ],
    "good_guy": true,
    "weight_kg": 71.6,
    "height_m": 1.72
}

JSON content

{
    "id": 1,
    "name": "Guionardo",
    "birthdate": "1977-02-05",
    "today": "2019-09-24 21:16:48.671843",
    "now": "23:20:22",
    "loved_languages": [
        "Python",
        "C#"
    ],
    "good_guy": true,
    "weight_kg": 71.6,
    "height_m": 1.72
}

AvroObject.data (object deserialized)
{'birthdate': '1977-02-05',
 'good_guy': True,
 'height_m': 1.72,
 'id': 1,
 'loved_languages': ['Python', 'C#'],
 'name': 'Guionardo',
 'now': '23:20:22',
 'today': '2019-09-24 21:16:48.671843',
 'weight_kg': 71.6}

#2: JSON from examples/good_guy.json

JSON content
{
    "id": 1,
    "name": "Guionardo",
    "birthdate": "1977-02-05",
    "today": "2019-09-24 21:16:48.671843",
    "now": "23:20:22",
    "loved_languages": [
        "Python",
        "C#"
    ],
    "good_guy": true,
    "weight_kg": 71.6,
    "height_m": 1.72
}

AvroObject.data (object deserialized)
{'birthdate': '1977-02-05',
 'good_guy': True,
 'height_m': 1.72,
 'id': 1,
 'loved_languages': ['Python', 'C#'],
 'name': 'Guionardo',
 'now': '23:20:22',
 'today': '2019-09-24 21:16:48.671843',
 'weight_kg': 71.6}

#3: JSON from https://support.oneskyapp.com/hc/en-us/article_attachments/202761627/example_1.json

JSON content
{
    "fruit": "Apple",
    "size": "Large",
    "color": "Red"
}

AvroObject.data (object deserialized)
{'color': 'Red', 'fruit': 'Apple', 'size': 'Large'}
```