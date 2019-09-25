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
