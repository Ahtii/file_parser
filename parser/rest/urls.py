from django.urls import path
from parser.rest import views


urlpatterns = [
    path('files/csv/upload', views.UploadCsvFileAPIView.as_view(), name='upload_csv_file'),
    path('files/csv/<str:file_name>/parse', views.ParseCsvFileToDatabaseAPIView.as_view(), name='parse_csv_file'),
    path('files/csv/<str:file_name>', views.CsvDatabaseFileAPIView.as_view(), name='csv_file')
]
