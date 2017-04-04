import datetime
from tap_harvest import utils


def _transform_datetime(value):
    return utils.strftime(datetime.datetime.utcfromtimestamp(int(value) * 0.001))


def _transform_object(data, prop_schema):
    return {k: transform(v, prop_schema[k]) for k, v in data.items() if k in prop_schema}


def _transform_array(data, item_schema):
    return [transform(row, item_schema) for row in data]


def _transform(data, typ, schema):
    if "format" in schema and typ != "null":
        if schema["format"] == "date-time":
            try:
                data = _transform_datetime(data)
            except Exception as e:
                data = str(data)

    if typ == "object":
        data = _transform_object(data, schema["properties"])

    if typ == "array":
        data = _transform_array(data, schema["items"])

    if typ == "null":
        if data is None or data == "":
            return None
        else:
            raise ValueError("Not null")

    if typ == "string":
        data = str(data)

    if typ == "integer":
        if isinstance(data, str):
            data = data.replace(',', '')
        data = int(data)

    if typ == "number":
        if isinstance(data, str):
            data = data.replace(',', '')
        data = float(data)

    if typ == "boolean":
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
        except Exception as e:
            pass

    raise Exception("Invalid data: {} does not match {}".format(data, schema))
