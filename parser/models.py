from django.db import models

class Company(models.Model):
    name = models.CharField(max_length=255)
    num_of_employees = models.PositiveIntegerField()
    website_url = models.URLField()

    class Meta:
        db_table = "company"

    def __str__(self):
        return self.name
