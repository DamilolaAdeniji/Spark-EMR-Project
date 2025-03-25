import sys

sys.path.append("/Users/Oluwadamilola/Documents/Data Engineering bootcamp/Spark-EMR-Project/")

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from core.spark_transformation import spark_transform

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("pytest-pyspark-testing") \
        .master("local[2]") \
        .getOrCreate()


def test_spark_transform(spark):
    sample_data = [
        ("123 Main St", "Agency X", "03/20/2024 08:30:00 AM", "03/20/2024 09:00:00 AM",
         "03/20/2024 09:10:00 AM", "37.7749", "-122.4194", "POINT(-122.4194 37.7749)",
         "GeomPoint", "03/20/2024 08:00:00 AM", "Service details", "Graffiti",
         "REQ123", "Subtype A", "Phone", "Closed", "Resolved", "03/20/2024 10:00:00 AM",
         "Neighborhood A", "BOS District", "http://media.url", "SF Boundaries",
         "Police District 1", "Main St", "Supervisor 1")
    ]

    schema = StructType([
        StructField("address", StringType()),
        StructField("agency_responsible", StringType()),
        StructField("closed_date", StringType()),
        StructField("data_as_of", StringType()),
        StructField("data_loaded_at", StringType()),
        StructField("lat", StringType()),
        StructField("long", StringType()),
        StructField("point", StringType()),
        StructField("point_geom", StringType()),
        StructField("requested_datetime", StringType()),
        StructField("service_details", StringType()),
        StructField("service_name", StringType()),
        StructField("service_request_id", StringType()),
        StructField("service_subtype", StringType()),
        StructField("source", StringType()),
        StructField("status_description", StringType()),
        StructField("status_notes", StringType()),
        StructField("updated_datetime", StringType()),
        StructField("analysis_neighborhood", StringType()),
        StructField("bos_2012", StringType()),
        StructField("media_url", StringType()),
        StructField("neighborhoods_sffind_boundaries", StringType()),
        StructField("police_district", StringType()),
        StructField("street", StringType()),
        StructField("supervisor_district", StringType()),
    ])

    df = spark.createDataFrame(sample_data, schema)

    result_df = spark_transform(df)

    # Check that all required columns exist
    expected_columns = [
        "address", "agency_responsible", "closed_date", "data_as_of", "data_loaded_at",
        "lat", "long", "point", "point_geom", "requested_datetime", "service_details",
        "service_name", "service_request_id", "service_subtype", "source",
        "status_description", "status_notes", "updated_datetime",
        "analysis_neighborhood", "bos_2012", "media_url", "neighborhoods_sffind_boundaries",
        "police_district", "street", "supervisor_district"
    ]

    assert result_df.columns == expected_columns

    # Check datatypes of transformed columns
    assert dict(result_df.dtypes)["closed_date"] == "timestamp"
    assert dict(result_df.dtypes)["lat"] == "double"
    assert dict(result_df.dtypes)["long"] == "double"
