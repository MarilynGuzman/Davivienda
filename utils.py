from impala.dbapi import connect
from decimal import Decimal
import json
import subprocess as sp
from pprint import pprint


def load_config():
    with open('config.json') as f:
        return json.load(f)


def load_query_metadata():
    with open('query_metadata.json') as f:
        return json.load(f)


def get_impala_cursor(host):
    conn = connect(host=host, port=21051, use_ssl=True, ca_cert='/opt/cloudera/security/pki/caroot-davivienda.pem',
                   auth_mechanism='GSSAPI', kerberos_service_name='impala')
    cursor = conn.cursor()
    return cursor


def results_to_array(description, results):
    try:
        if results[0][0] == 'error':
            return [{'error': results[0][1]}]
    except:
        pass
    if len(results) == 0:
        return []
    tuple_len = len(results[0])
    data = []
    for i in range(len(results)):
        temp = {}
        for j in range(tuple_len):
            if type(results[i][j]) is not Decimal:
                temp[description[j][0]] = results[i][j]
            else:
                temp[description[j][0]] = float(results[i][j])
        data.append(temp)
    return data


def results_to_json(description, results):
    tuple_len = len(results[0])
    temp = {}
    for j in range(tuple_len):
        temp[description[j][0]] = results[0][j]
    return temp


def json_to_string(json_object, encoding='utf-8'):
    pprint(json_object)
    print type(json_object)
    return json.dumps(json_object, sort_keys=True, indent=4, separators=(',', ': '), encoding=encoding, ensure_ascii=False)


def load_error_message(error):
    print error
    return json.dumps(u"{}".format(error), sort_keys=True, indent=4, separators=(',', ': '), ensure_ascii=False).encode('utf-8')
    # error_message = str(error)
    # error_message = error_message.replace('u\'', '\'')
    # error_message = error_message.replace('\'', '"')
    # return json.loads(error_message)


def lstring(list, sep=None):
    if sep is None:
        return "".join(list)
    return sep.join(list)


def kerberos_initializer(user):
    kinit_executable = "kinit"
    keytab = """{user}.keytab""".format(user=user)
    cmd = [
        kinit_executable,
        "-kt",
        keytab,
        user
    ]
    proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, shell=False)
    proc.wait()
    if proc.returncode != 0:
        print("--Unrecoverable error, kerberos initialization failed")
        print(lstring(proc.stdout.readlines()))
        exit(1)
    else:
        print("--Kerberos initialization successful")
