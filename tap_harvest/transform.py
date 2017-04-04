import dateparser
from singer import utils

EXPECTED_DATE_FORMATS = ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S']

def _transform_datetime(value):
    return utils.strftime(dateparser.parse(value, date_formats=EXPECTED_DATE_FORMATS))


def _transform_object(data, prop_schema):
    return {k: transform(v, prop_schema[k]) for k, v in data.items() if k in prop_schema}


def _transform_array(data, item_schema):
    return [transform(row, item_schema) for row in data]


def _transform(data, typ, schema):
    if "format" in schema and typ != "null":
        if schema["format"] == "date-time":
            try:
                data = _transform_datetime(data)
            except Exception:
                data = str(data)

    elif typ == "object":
        data = _transform_object(data, schema["properties"])

    elif typ == "array":
        data = _transform_array(data, schema["items"])

    elif typ == "null":
        if data is None or data == "":
            return None
        else:
            raise ValueError("Not null")

    elif typ == "string":
        data = str(data)

    elif typ == "integer":
        if isinstance(data, str):
            data = data.replace(',', '')
        data = int(data)

    elif typ == "number":
        if isinstance(data, str):
            data = data.replace(',', '')
        data = float(data)

    elif typ == "boolean":
        data = bool(data)

    return data


def transform(data, schema):
    types = schema["type"]
    if not isinstance(types, list):
        types = [types]

    if "null" in types:
        types.remove("null")
        types.append("null")

    for typ in types:
        try:
            return _transform(data, typ, schema)
        except Exception:
            pass

    raise Exception("Invalid data: {} does not match {}".format(data, schema))
