import os
import shelve

import db_api
from dataclasses import dataclass
from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class DBField(db_api.DBField):
    def __init__(self, name, type):
        self.name = name
        self.type = type


@dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    def __init__(self, name, fields, key_field_name):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name

        path_file = os.path.join('db_files', self.table_name + '.db')
        table = shelve.open(path_file)
        values = {}
        table[key_field_name] = values
        for field in fields:
            values[field] = ""
        table.close()

    def count(self):
        table = shelve.open(os.path.join('db_files', self.name + '.db'))
        try:
            num_rows = len(table.keys())
            # [self.name]
        finally:
            table.close()
        return num_rows

    def insert_record(self, values):
        primary_key = None
        if values is None or len(values) > len(self.fields):
            raise ValueError

        for val in values:
            if val == self.key_field_name:
                primary_key = values[val]
                continue
            else:
                raise ValueError

        table = shelve.open(os.path.join('db_files', self.name + '.db'), writeback=True)
        try:
            if primary_key not in table:
                table[primary_key] = values
            else:
                raise ValueError
        finally:
            table.close()

    def delete_record(self, key: Any):
        raise NotImplementedError

    def delete_records(self, criteria: List[SelectionCriteria]):
        raise NotImplementedError

    def get_record(self, key: Any):
        raise NotImplementedError

    def update_record(self, key: Any, values: Dict[str, Any]):
        raise NotImplementedError

    def query_table(self, criteria: List[SelectionCriteria]):
        raise NotImplementedError

    def create_index(self, field_to_index: str):
        raise NotImplementedError


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    tables = {}

    def create_table(self, table_name, fields, key_field_name):
        if table_name not in self.tables:
            self.tables[table_name] = DBTable(table_name, fields, key_field_name)
            return self.tables[table_name]
        else:
            raise ValueError

    def num_tables(self):
        return len(self.tables)

    def get_table(self, table_name):
        return self.tables.get(table_name)

    def delete_table(self, table_name):
        if table_name in self.tables:
            os.remove(db_api.DB_ROOT.joinpath(f"{table_name}.dir"))
            self.tables.pop(table_name)
        else:
            raise ValueError

    def get_tables_names(self):
        return list(self.tables.keys())

    def query_multiple_tables(self, tables: List[str], fields_and_values_list: List[List[SelectionCriteria]], fields_to_join_by: List[str]) -> List[Dict[str, Any]]:
        raise NotImplementedError


















    # להריץ
    # טסט
    # מסוים:
    # py.test - k
    # test_name
    # להריץ
    # עם
    # מדידת
    # זמן:
    # pytest - -durations = 8
