import os
import shelve

import db_api
from dataclasses import dataclass
from dataclasses_json import dataclass_json

table_list = []

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

        def count(self):
            raise NotImplementedError

        def insert_record(self, values: Dict[str, Any]):
            raise NotImplementedError

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
class DataBase:
# Put here any instance information needed to support the API
    def create_table(self, table_name: str, fields: List[DBField], key_field_name: str) -> DBTable:
            raise NotImplementedError

    def num_tables(self) -> int:
            raise NotImplementedError

    def get_table(self, table_name: str) -> DBTable:
            raise NotImplementedError

    def delete_table(self, table_name: str) -> None:
            raise NotImplementedError

    def get_tables_names(self) -> List[Any]:
            raise NotImplementedError

    def query_multiple_tables(self, tables: List[str], fields_and_values_list: List[List[SelectionCriteria]], fields_to_join_by: List[str]) -> List[Dict[str, Any]]:
            raise NotImplementedError


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
# Put here any instance information needed to support the API
    def create_table(self, table_name, fields, key_field_name):
        value = {}
        for field in fields:
            value[field] = ''
        path_file = os.path.join('db_files', table_name + '.db')
        table_file = shelve.open(path_file)
        table_file[key_field_name] = value
        table_file.close()
        table_list.append(table_name)

    def num_tables(self):
        return len(table_list)

    def get_table(self, table_name: str) -> DBTable:
        raise NotImplementedError

    def delete_table(self, table_name: str) -> None:
        raise NotImplementedError

    def get_tables_names(self) -> List[Any]:
        raise NotImplementedError

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
