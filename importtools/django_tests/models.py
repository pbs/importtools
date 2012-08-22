from django.db import models


class TestModel(models.Model):
    a = models.IntegerField()
    b = models.CharField(max_length=10)
    x = models.BooleanField()
    y = models.CharField(max_length=10)
