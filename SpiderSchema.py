import pandas as pd

class SpiderSchema:
    """
    Class to handle the spider dataset schemas.
    """
    def __init__(self, table_json: str):
        self.schema, self.primary, self.foreign = self.get_spider_schemas(table_json)

    def find_db_fields(self, db_name: str):
        """
        Returns the fields of the database with the given name as a string.
        """
        df = self.schema[self.schema['Database name'] == db_name]
        df = df.groupby(' Table Name')
        output = ""
        for name, group in df:
            output += "Table " +name+ ', columns = ['
            for index, row in group.iterrows():
                output += row[" Field Name"]+','
            output = output[:-1]
            output += "]\n"
        return output

    def find_db_primary_keys(self, db_name: str):
        """
        Returns the primary keys of the database with the given name as a string.
        """
        df = self.primary[self.primary['Database name'] == db_name]
        output = "["
        for index, row in df.iterrows():
            output += row['Table Name'] + '.' + row['Primary Key'] +','
        if len(output)>1:
            output = output[:-1]
        output += "]\n"
        return output

    def find_db_foreign_keys(self, db_name: str):
        """
        Returns the foreign keys of the database with the given name as a string.
        """
        df = self.foreign[self.foreign['Database name'] == db_name]
        output = "["
        for index, row in df.iterrows():
            output += row['First Table Name'] + '.' + row['First Table Foreign Key'] + " = " + row['Second Table Name'] + '.' + row['Second Table Foreign Key'] + ','
        output= output[:-1] + "]"
        return output

    def get_db_schema(self, db_name: str) -> str:
        """
        Returns the schema of the database with the given name as a string.
        """
        fields = self.find_db_fields(db_name)
        fields += "Foreign_keys = " + self.find_db_primary_keys(db_name)
        fields += "Primary_keys = " + self.find_db_foreign_keys(db_name)
        return fields

    @staticmethod
    def get_spider_schemas(table_json: str):
        """
        Returns the schemas of the spider dataset as pandas dataframes, with the following columns:
        - spider_schema: Database name, Table Name, Field Name, Type
        - spider_primary: Database name, Table Name, Primary Key
        - spider_foreign: Database name, First Table Name, Second Table Name, First Table Foreign Key, Second Table Foreign Key

        :param table_json: path to the table.json file of the spider dataset
        :return: spider_schema, spider_primary, spider_foreign
        """
        schema_df = pd.read_json(table_json)
        schema_df = schema_df.drop(['column_names','table_names'], axis=1)
        schema = []
        f_keys = []
        p_keys = []
        for index, row in schema_df.iterrows():
            tables = row['table_names_original']
            col_names = row['column_names_original']
            col_types = row['column_types']
            foreign_keys = row['foreign_keys']
            primary_keys = row['primary_keys']
            for col, col_type in zip(col_names, col_types):
                index, col_name = col
                if index == -1:
                    for table in tables:
                        schema.append([row['db_id'], table, '*', 'text'])
                else:
                    schema.append([row['db_id'], tables[index], col_name, col_type])
            for primary_key in primary_keys:
                index, column = col_names[primary_key]
                p_keys.append([row['db_id'], tables[index], column])
            for foreign_key in foreign_keys:
                first, second = foreign_key
                first_index, first_column = col_names[first]
                second_index, second_column = col_names[second]
                f_keys.append([row['db_id'], tables[first_index], tables[second_index], first_column, second_column])
        spider_schema = pd.DataFrame(schema, columns=['Database name', ' Table Name', ' Field Name', ' Type'])
        spider_primary = pd.DataFrame(p_keys, columns=['Database name', 'Table Name', 'Primary Key'])
        spider_foreign = pd.DataFrame(f_keys,
                            columns=['Database name', 'First Table Name', 'Second Table Name', 'First Table Foreign Key',
                                    'Second Table Foreign Key'])
        return spider_schema, spider_primary, spider_foreign
