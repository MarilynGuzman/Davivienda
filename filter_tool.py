import json


def json_to_filter_query_original(json_string):
    json_array = json.loads(json_string)
    if len(json_array) == 0:
        return ''
    filter_query = """ WHERE {} {} {}""".format("".join(json_array[0]['column']), "".join(json_array[0]['comparator']), "".join(json_array[0]['value']))
    if len(json_array) > 1:
        for i in range(1, len(json_array)):
            filter_query += """ AND {} {} {}""".format("".join(json_array[i]['column']), "".join(json_array[i]['comparator']), "".join(json_array[i]['value']))
    return filter_query


def json_to_filter_query(json_string):
    json_array = json.loads(json_string)
    if len(json_array) == 0:
        return 'True'

    for item in json_array:
        try:
            int(item['value'])
        except:
            item['value'] = "\"{}\"".format(item['value'])

    filter_array = ["{} {} {}".format(item['column'], item['comparator'], item['value']) for item in json_array]

    filter_query = " AND ".join(filter_array)

    return filter_query
