from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response
from rest_framework import status

from parser.utls import UploadCsvFile, ParseCsvFileToDatabase
from parser.models import Company


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

    def post(self, request):
        file_path = request.data.get('file_path')

        if error_msg := ParseCsvFileToDatabase(file_path, Company).load_into_db():
            return Response({'error': error_msg})

        return Response({"message": "File loaded."}, status=status.HTTP_201_CREATED)


