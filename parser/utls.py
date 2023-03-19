import pandas as pd
import os

from django.conf import settings

from parser.constants import DEFAULT_FILE_CHUNK_SIZE, FILE_MODEL_NAME_MAPPER



class UploadCsvFile:

    def __init__(self, file):
        self.file = file

    def upload(self):
        file_path = error_msg = str()

        try:
            file_path = self.save_file()
        except Exception as error:
            error_msg, *_ = error.args

        return error_msg, file_path

    def save_file(self):
        file_name = self.file.name
        file_path = os.path.join(settings.CSV_FILE_PATH, file_name).replace("\\","/")

        if not file_name.endswith('.csv'):
            raise Exception("Invalid file format. Please upload csv file.")

        with open(file_path, 'w') as file_obj:
            file_obj.write(self.file.read().decode('utf-8'))

        return file_path


class ParseCsvFileToDatabase:
    """
    Parse the csv file into chunks and populate the chunk to database table

    """

    def __init__(self, file_name):
        self.model_name = FILE_MODEL_NAME_MAPPER.get(file_name)
        self.file_name = file_name

    def load_into_db(self):
        error_msg = str()
        file_path = self.get_csv_file_path(self.file_name)

        try:
            for chunk in pd.read_csv(file_path, chunksize=DEFAULT_FILE_CHUNK_SIZE):
                columns = chunk.columns
                model_objs = [
                    self.model_name(
                        **{column: row[column] for column in columns}
                    ) for _, row in chunk.iterrows()
                ]
                self.model_name.objects.bulk_create(model_objs)
        except FileNotFoundError as error:
            *_, error_msg = error.args
        except Exception as error:
            error_msg, *_ = error.args

        return error_msg

    @staticmethod
    def get_csv_file_path(file_name):
        return os.path.join(settings.CSV_FILE_PATH, f"{file_name}.csv").replace("\\","/")

class RenderCsvDatabaseFile:
    """
    Render the records from the database table based on given filters

    """

    def __init__(self, file_name):
        self.model_name = FILE_MODEL_NAME_MAPPER.get(file_name)
        self.file_name = file_name

    def fetch_records(self, filters):
        error_msg = str()
        records = dict()

        try:
            record_limit = int(filters.get('limit', 50))

            if (sort_by := filters.get('column_name')) and (direction := filters.get('direction')):
                sort_by = f"-{sort_by}" if direction.lower() == 'descending' else sort_by
                records = self.model_name.objects.order_by(sort_by)
            else:
                records = self.model_name.objects.all()

            records = {self.file_name: records[:record_limit].values()}
        except Exception as error:
            error_msg, *_ = error.args

        return error_msg, records
