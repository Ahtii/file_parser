import pandas as pd

from django.conf import settings

from parser.constants import DEFAULT_FILE_CHUNK_SIZE



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
        file_path = f"{settings.CSV_FILE_PATH}/{file_name}"

        if not file_name.endswith('.csv'):
            raise Exception("Invalid file format. Please upload csv file.")

        with open(file_path, 'w') as file_obj:
            file_obj.write(self.file.read().decode('utf-8'))

        return file_path


class ParseCsvFileToDatabase:
    """
    Parse the csv file into chunks and populate the chunk to database table

    """

    def __init__(self, file_path, model_name):
        self.file_path = file_path
        self.model_name = model_name

    def load_into_db(self):
        error_msg = str()

        try:
            for chunk in pd.read_csv(self.file_path, chunksize=DEFAULT_FILE_CHUNK_SIZE):
                columns = chunk.columns
                model_objs = [self.model_name(**{column: row[column] for column in columns}) for _, row in chunk.iterrows()]
                self.model_name.objects.bulk_create(model_objs)
        except FileNotFoundError as error:
            *_, error_msg = error.args
        except Exception as error:
            error_msg, *_ = error.args

        return error_msg
