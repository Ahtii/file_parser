from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response
from rest_framework import status

from parser.utls import UploadCsvFile, ParseCsvFileToDatabase, RenderCsvDatabaseFile


class UploadCsvFileAPIView(APIView):
    """
    Upload the csv file in specified location.

    """
    parser_classes = (MultiPartParser, FormParser)

    def post(self, request):
        file = request.data.get('csv_file')
        error_msg, file_path = UploadCsvFile(file).upload()

        if error_msg:
            return Response({'error': error_msg})

        return Response({"message": "File uploaded."}, status=status.HTTP_201_CREATED)


class ParseCsvFileToDatabaseAPIView(APIView):
    """
    Parse and load csv file into database table.

    """

    def post(self, request, *args, **kwargs):
        file_name = kwargs.get('file_name')

        if error_msg := ParseCsvFileToDatabase(file_name).load_into_db():
            return Response({'error': error_msg})

        return Response({"message": "File loaded."}, status=status.HTTP_201_CREATED)


class CsvDatabaseFileAPIView(APIView):
    """
    Render database rows based on filters.

    """

    def get(self, request, *args, **kwargs):
        file_name = kwargs.get('file_name')
        error_msg, records = RenderCsvDatabaseFile(file_name).fetch_records(request.query_params)

        if error_msg:
            return Response({'error': error_msg})

        return Response(records)


