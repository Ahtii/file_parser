from rest_framework.views import APIView


class CSVFileAPIView(APIView):
    """
    Parse and insert records in the csv file

    """

    def post(self, request):
        file = request.FILES.get('file')
