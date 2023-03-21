from os.path import join as path_join

from django.conf import settings

from parser.constants import DEFAULT_FILE_CHUNK_SIZE, FILE_MODEL_NAME_MAPPER, DEFAULT_RECORD_LIMIT, SORT_DIRECTION_DESC

from pandas import read_csv

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
        file_path = self.get_csv_file_path(file_name)

        if not file_name.endswith('.csv'):
            raise Exception("Invalid file format. Please upload csv file.")

        with open(file_path, 'w') as file_obj:
            file_obj.write(self.file.read().decode('utf-8'))

        return file_path

    @staticmethod
    def get_csv_file_path(file_name):
        file_name = file_name.split(".")[0] if file_name.endswith('.csv') else file_name
        return path_join(settings.CSV_FILE_PATH, f"{file_name}.csv").replace("\\","/")


class ParseCsvFileToDatabase:
    """
    Parse the csv file into chunks and populate the chunk to database table

    """

    def __init__(self, file_name):
        self.model_name = FILE_MODEL_NAME_MAPPER.get(file_name)
        self.file_name = file_name

    def load_into_db(self):
        error_msg = str()
        file_path = UploadCsvFile.get_csv_file_path(self.file_name)
        print("f: ", file_path)

        try:
            self.populate_db(file_path)
        except FileNotFoundError as error:
            *_, error_msg = error.args
        except Exception as error:
            error_msg, *_ = error.args

        return error_msg

    def populate_db(self, file_path):
        for chunk in read_csv(file_path, chunksize=DEFAULT_FILE_CHUNK_SIZE):
            columns = chunk.columns
            model_objs = [
                self.model_name(
                    **{column: row[column] for column in columns}
                ) for _, row in chunk.iterrows()
            ]
            self.model_name.objects.bulk_create(model_objs)



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
            records = self.get_filtered_records(filters)
        except Exception as error:
            error_msg, *_ = error.args

        return error_msg, records

    def get_filtered_records(self, filters):
        record_limit = int(filters.get('limit', DEFAULT_RECORD_LIMIT))
        sort_by = filters.get('column_name')
        direction = filters.get('direction')

        if sort_by and direction:
            sort_by = f"-{sort_by}" if direction.lower() == SORT_DIRECTION_DESC else sort_by
            records = self.model_name.objects.order_by(sort_by)
        else:
            records = self.model_name.objects.all()

        return {self.file_name: records[:record_limit].values()}
