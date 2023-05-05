import json
from sqlite_tables import insert_data_to_sqlite, db_path, table_name, enrich_table


def load_table_from_json(filename: str, table_name: str = table_name, db_path: str = db_path, truncate: bool = False):
    with open(filename, 'r') as f:
        lines = f.readlines()
        lines = [line.strip() for line in lines]
        lines = [line for line in lines if line]
        lines = [line for line in lines if line[0] == '{']
        lines = [json.loads(line) for line in lines]
        insert_data_to_sqlite(db_path=db_path, table_name=table_name, data_dict=lines, truncate=truncate)
        return True

load_table_from_json(filename = 'MyFunctionProj\MyHttpTrigger\diag_log_1.json', table_name = table_name, db_path = db_path, truncate = True)
load_table_from_json(filename = 'MyFunctionProj\MyHttpTrigger\diag_del_log.json', table_name = table_name, db_path = db_path, truncate = False)
load_table_from_json(filename = 'MyFunctionProj\MyHttpTrigger\diag_read_log.json', table_name = table_name, db_path = db_path, truncate = False)
load_table_from_json(filename = 'MyFunctionProj\MyHttpTrigger\diag_read_log2.json', table_name = table_name, db_path = db_path, truncate = False)
load_table_from_json(filename = 'MyFunctionProj\MyHttpTrigger\parse_read_new.json', table_name = table_name, db_path = db_path, truncate = False)

enrich_table(db_path = db_path, table_name = table_name)
