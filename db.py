# import db_api
from .db_files import *


@dataclass_json
@dataclass
class DBField:
    def __init__(self, name, type):
        self.name = name
        self.type = type


@dataclass_json
@dataclass
class SelectionCriteria:
    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value


@dataclass_json
@dataclass
class DBTable:
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
class DataBase(db_api.DataBase):
    להריץ
    טסט
    מסוים:
    py.test - k
    test_name
    להריץ
    עם
    מדידת
    זמן:
    pytest - -durations = 8
