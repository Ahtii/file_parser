from django.urls import path
from parser.rest import views


urlpatterns = [
    path('files/csv/', views.CSVFileAPIView.as_view(), name='csv_file_view'),
]
