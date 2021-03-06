from fastavro import writer, reader, schema
from rec_avro import to_rec_avro_destructive, from_rec_avro_destructive, rec_avro_schema

def json_objects():
    return [{'a': 'a'}, {'b':'b'}]

# For efficiency, to_rec_avro_destructive() destroys rec, and reuses it's
# data structures to construct avro_objects 
avro_objects = (to_rec_avro_destructive(rec) for rec in json_objects())

# store records in avro
with open('json_in_avro.avro', 'wb') as f_out:
    writer(f_out, schema.parse_schema(rec_avro_schema()), avro_objects)

#load records from avro
with open('json_in_avro.avro', 'rb') as f_in:
    # For efficiency, from_rec_avro_destructive(rec) destroys rec, and 
    # reuses it's data structures to construct it's output
    loaded_json = [from_rec_avro_destructive(rec) for rec in reader(f_in)]

assert loaded_json == json_objects()