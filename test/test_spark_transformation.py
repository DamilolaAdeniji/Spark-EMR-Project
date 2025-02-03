import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("spark_transformation").getOrCreate()
        
    yield spark
    spark.stop()

    # TO DO - write test