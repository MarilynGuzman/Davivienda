import subprocess as sp
import json
from sys import argv
from time import sleep
import re
import pandas as pd
from datetime import datetime, timedelta

def load_config(config_file="config.json"):
    with open(config_file) as f:
        return json.load(f)


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


def impala_shell_execute(query):
    cmd = [
        'impala-shell',
        "-k",
        "--ssl",
        "-i",
        config['impala_host'],
        "-q",
        "{}".format(query)
    ]
    print " ".join(cmd)
    proc = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.STDOUT, shell=False)
    out, err = proc.communicate()
    if proc.returncode != 0:
        print("--Query failed")
        print(err)
        print(out)
        exit(1)
    else:
        if out is not None:
            print("--Query execution success")
            print(out)
            out_table = parse_table(out)
            print out_table
            return out_table


def parse_table(out):
    horizontal_lines_re = r"(\+\-+)+\+"
    out_lines = out.split("\n")
    while not re.search(horizontal_lines_re, out_lines[0]):
        del out_lines[0]
        if len(out_lines) == 0:
            return None
    columns = map(lambda x: x.strip(), out_lines[1].split('|')[1:-1])
    print columns
    data = out_lines[3:-3]
    data = map(lambda x: map(lambda y: y.strip(),  x.split('|')[1:-1]), data)
    return pd.DataFrame(data, columns=columns)


def get_columns_list(table_name):
    db = table_name.split('.')[0].upper()
    table = table_name.split('.')[1].upper()
    produccion = db != '8096_tec_planeacion_y_riesgo'.upper()
    
    table_name = table_name.upper()
    get_columns_list_query = """
                             SELECT group_concat(nombre_campo, ', ') as columns_list
                             FROM {db}.fuentes_cloudera
                             JOIN {db}.campos_fuentes_cloudera
                                 ON (fuentes_cloudera.codigo = campos_fuentes_cloudera.codigo_fuente)
                             WHERE nombre_tabla = '{table_name}'
                             AND database_destino LIKE '%{database_name}'
                             """.format(
                                        table_name=table,
                                        database_name=db.lower() if produccion else db,
                                        db='8096_tec_planeacion_y_riesgo' if not produccion else 'utilitarios'
                             )
    columns_list = impala_shell_execute(get_columns_list_query).iloc[0]['columns_list']
    return ", ".join(reversed(columns_list.split(', ')))


def change_columns_cifrado(columns_list):
    columns_array = map(lambda x: x.strip(), columns_list.split(','))
    columns_array_cifrado = map(lambda column: "{}_CIFRADO".format(column) if "{}_CIFRADO".format(column) in columns_array else column, columns_array)
    return ", ".join(columns_array_cifrado)


def table_migrator(origin_table_name, destination_table_name, periodo_inicial, periodo_final, cifrado=False):
    periodo_datetime = datetime.strptime(periodo_inicial, "%Y%m%d")

    columns_list = get_columns_list(origin_table_name)

    columns_list = change_columns_cifrado(columns_list) if cifrado else columns_list

    if(int(periodo_final) < int(periodo_inicial)):
        print("--periodos invalidos")
        raise Exception
    periodo_final_plus_one = datetime.strptime(periodo_final, "%Y%m%d") + timedelta(days=1)
    periodo_final_plus_one = periodo_final_plus_one.strftime("%Y%m%d")
    while(periodo_datetime.strftime("%Y%m%d") !=  periodo_final_plus_one):
        periodo = periodo_datetime.strftime("%Y%m%d")
        insert_query = """
        INSERT OVERWRITE {destination_table_name}
        PARTITION (periodo = {periodo})
        SELECT {columns_list}
        FROM {origin_table_name}
        WHERE periodo = {periodo};
        """.format(origin_table_name=origin_table_name, destination_table_name=destination_table_name, periodo=periodo, columns_list=columns_list)
        periodo_datetime += timedelta(days=1)
        impala_shell_execute(insert_query)


def get_last_period(query_info, query_info_space=False):

    final_query_info = query_info.split('.')
    lower_info = [x.lower() for x in query_info.split('.') if x != '' ]
    flag = False
    if not query_info_space and len(lower_info) == 2:
        db = lower_info[0]
        table = lower_info[1]
    elif len(lower_info) == 1 and query_info_space:
        db = query_info.lower().strip('.')
        table = query_info_space.lower().strip('.')
    else:
        raise Exception("--Argumento invalido, debe ingresar el nombre de la db y el nombre de la tabla ej <my_data_base.my_table>")
    return impala_shell_execute(config['max_periodo_query'].format(db = db, table = table)).iloc[0]['max_periodo']

def get_table_file_name(table_name):
    return impala_shell_execute(config['file_name_query'].format(table = table_name)).iloc[0]['nombre_archivo']


def update_to_last_period(origin_table_name, destination_table_name, periodo_final=None, cifrado=False):
    last_updated_period = datetime.strptime(get_last_period(destination_table_name), "%Y%m%d") + timedelta(days=1)
    periodo_inicial = last_updated_period.strftime("%Y%m%d")
    if periodo_final is None:
        today = datetime.now() + timedelta(days=-1)
        periodo_final = today.strftime("%Y%m%d")

    table_migrator(origin_table_name, destination_table_name, "20190424", periodo_final, cifrado)
config = load_config()



