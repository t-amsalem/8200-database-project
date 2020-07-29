import os
import shelve
import db_api
from dataclasses import dataclass
from dataclasses_json import dataclass_json
import operator


operators = {
    '<': operator.lt,
    '<=': operator.le,
    '==': operator.eq,
    '=': operator.eq,
    '!=': operator.ne,
    '>=': operator.ge,
    '>': operator.gt
}


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
        if key_field_name not in [field.name for field in fields]:
            raise ValueError
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name
        path_file = os.path.join('db_files', self.name)
        table = shelve.open(path_file)
        table.close()

    def count(self):
        table_file = shelve.open(os.path.join('db_files', self.name))
        try:
            count_rows = len(table_file.keys())
        finally:
            table_file.close()
        return count_rows

    def insert_record(self, values):
        if values is None or len(values) > len(self.fields) or values[self.key_field_name] is None:
            raise ValueError("bad index")
        table = shelve.open(os.path.join('db_files', self.name), writeback=True)
        try:
            if table.get(str(values[self.key_field_name])):
                table.close()
                raise ValueError("already exist")
            else:
                table[str(values[self.key_field_name])] = values
        finally:
            table.close()

    def delete_record(self, key):
        if key is None:
            raise ValueError
        table = shelve.open(os.path.join('db_files', self.name), writeback=True)
        try:
            if str(key) not in table:
                raise ValueError
            del table[str(key)]
        finally:
            table.close()

    def delete_records(self, criteria):
        table = shelve.open(os.path.join('db_files', self.name), writeback=True)
        try:
            for key in table:
                condition = ""
                for cond in criteria:
                    str_operator = operators.get(cond.operator)
                    if cond.field_name == self.key_field_name:
                        condition += str(str_operator(key, str(cond.value))) + " and "
                    else:
                        condition += str(str_operator(str(table[key][cond.field_name]), str(cond.value))) + " and "
                if eval(condition[:-4]):
                    del table[key]
        finally:
            table.close()

    def get_record(self, key):
        if key is None:
            raise ValueError("bad index")
        table = shelve.open(os.path.join('db_files', self.name), writeback=True)
        try:
            if str(key) not in table:
                raise ValueError("bad index")
            else:
                value = table[str(key)]
                table.close()
                return value
        finally:
            table.close()

    def update_record(self, key, values):
        if key is None:
            raise ValueError("bad index")
        table = shelve.open(os.path.join('db_files', self.name), writeback=True)
        try:
            if str(key) not in table:
                raise ValueError("bad index")
            else:
                table[str(key)] = values
        finally:
            table.close()

    def query_table(self, criteria):
        table = shelve.open(os.path.join('db_files', self.name), writeback=True)
        satisfies_cond = {}
        list_satisfies_cond = []
        try:
            for key in table:
                condition = ""
                for cond in criteria:
                    str_operator = operators.get(cond.operator)
                    condition += str(str_operator(str(table[key][cond.field_name]), str(cond.value))) + " and "
                if eval(condition[:-4]):
                    satisfies_cond[self.key_field_name] = key
                    satisfies_cond.update(table[key])
                    list_satisfies_cond.append(satisfies_cond)
        finally:
            table.close()
        return list_satisfies_cond

    def create_index(self, field_to_index):
        if field_to_index not in [field.name for field in self.fields]:
            raise ValueError
        if field_to_index == self.key_field_name:
            return
        with shelve.open(f'{self.name}_{field_to_index}_index', writeback=True) as index_file:
            table = shelve.open(os.path.join('db_files', self.name), writeback=True)
            all_instances = {}
            for key, value in table.items():
                all_instances.setdefault(value[field_to_index], set()).add(key)
            index_file[field_to_index] = all_instances


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    def __init__(self):
        self.tables = {}
        db_file = shelve.open('DB', writeback=True)
        for key in db_file:
            self.tables[key] = DBTable(key, db_file[key][0], db_file[key][1])

    def create_table(self, table_name, fields, key_field_name):
        if table_name not in self.tables:
            self.tables[table_name] = DBTable(table_name, fields, key_field_name)
            with shelve.open('DB', writeback=True) as db:
                db[table_name] = [fields, key_field_name]
        return self.tables[table_name]

    def num_tables(self):
        return len(self.tables)

    def get_table(self, table_name):
        if table_name not in self.tables:
            raise ValueError("bad index")
        return self.tables.get(table_name)

    def delete_table(self, table_name):
        if table_name in self.tables:
            os.remove(db_api.DB_ROOT.joinpath(f"{table_name}.dir"))
            self.tables.pop(table_name)
        else:
            raise ValueError("bad index")

    def get_tables_names(self):
        return list(self.tables.keys())

    def query_multiple_tables(self, tables, fields_and_values_list, fields_to_join_by):
        pass
