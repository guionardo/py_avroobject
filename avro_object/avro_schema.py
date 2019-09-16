import pprint
import numbers


def create_schema(origin: dict, name: str, namespace: str = 'avro.example'):
    '''
    Create schema from object
    '''
    o_type = None
    if isinstance(origin, dict):
        o_type = 'record'
    elif isinstance(origin, list):
        o_type = 'array'
    elif isinstance(origin, str):
        o_type = 'string'
    elif isinstance(origin, numbers.Integral):
        o_type = 'int'
    elif isinstance(origin, numbers.Real):
        o_type = 'float'
    else:
        return None
    if not name:
        return None
    schema = {
        'namespace': namespace,
        'type': o_type,
        'name': name,
        'fields': []
    }

    for k, v in origin.items():
        field = create_schema(origin=v, name=k)
        schema['fields'].append(field)

    return schema



