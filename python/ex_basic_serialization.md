# Basic Serialization

Just an dict to JSON

``` python
# Example: Basic Serialization
#
# Just an dict to JSON
#
from avro_object import AvroObject
from datetime import datetime, date, time
from pprint import pprint

print("Basic Serialization\n")

obj = {
    "id": 1,
    "name": "Guionardo",
    "birthdate": date(1977, 2, 5),
    "today": datetime.today(),
    "now": time(23, 20, 22),
    "loved_languages": ["Python", "C#"],
    "good_guy": True,
    "weight_kg": 71.6,
    "height_m": 1.72
}
print("Object")
pprint(obj)
ao = AvroObject(obj)

print("\nJSON")
pprint(ao.to_json())
```

## OUTPUT

```
Basic Serialization

Object
{'birthdate': datetime.date(1977, 2, 5),
 'good_guy': True,
 'height_m': 1.72,
 'id': 1,
 'loved_languages': ['Python', 'C#'],
 'name': 'Guionardo',
 'now': datetime.time(23, 20, 22),
 'today': datetime.datetime(2019, 9, 30, 9, 0, 54, 407132),
 'weight_kg': 71.6}

JSON
('{"id": 1, "name": "Guionardo", "birthdate": "1977-02-05", "today": '
 '"2019-09-30 09:00:54.407132", "now": "23:20:22", "loved_languages": '
 '["Python", "C#"], "good_guy": true, "weight_kg": 71.6, "height_m": 1.72}')
 ```