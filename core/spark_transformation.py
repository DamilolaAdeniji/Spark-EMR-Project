from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s')

spark = SparkSession.builder.appName("spark_transformation").getOrCreate()  # noqa

data = spark.read.parquet("s3://final-aws-project/raw/311-calls-data.parquet") # noqa

logging.info(
    'Loaded data from s3://final-aws-project/raw/311-calls-data.parquet'  # noqa
    )

# TODO : filter the data to only include the following columns


def spark_transform(data):
    df_filtered = data.select(
        "address",
        "agency_responsible",
        "closed_date",
        "data_as_of",
        "data_loaded_at",
        "lat",
        "long",
        "point",
        "point_geom",
        "requested_datetime",
        "service_details",
        "service_name",
        "service_request_id",
        "service_subtype",
        "source",
        "status_description",
        "status_notes",
        "updated_datetime",
        "analysis_neighborhood",
        "bos_2012",
        "media_url",
        "neighborhoods_sffind_boundaries",
        "police_district",
        "street",
        "supervisor_district"
    )

    df_filtered = (
        df_filtered.withColumn("closed_date", to_timestamp(col("closed_date"), # noqa
                                "MM/dd/yyyy hh:mm:ss a")).withColumn("data_as_of", # noqa
                                to_timestamp(col("data_as_of"), "MM/dd/yyyy hh:mm:ss a") # noqa
                                ).withColumn("data_loaded_at", to_timestamp(col("data_loaded_at"), # noqa
                                "MM/dd/yyyy hh:mm:ss a")) .withColumn("requested_datetime", # noqa
                                to_timestamp(col("requested_datetime"),  # noqa
                                "MM/dd/yyyy hh:mm:ss a")) .withColumn("updated_datetime", # noqa
                                to_timestamp(col("updated_datetime"),  # noqa
                                "MM/dd/yyyy hh:mm:ss a")) .withColumn("lat",  # noqa
                                col("lat").cast("double")) .withColumn("long",  # noqa
                                col("long").cast("double"))) # noqa
# TODO : output the transformed data to an s3 bucket

    logging.info(
    'Data transformed successfully'  # noqa
    )

    return df_filtered


if __name__ == "__main__":
    spark_transform(data).write.parquet(
        "s3://final-aws-project/transformed/311-calls-data.parquet")   # noqa
    logging.info('Data exported to s3://final-aws-project/transformed/311-calls-data.parquet')   # noqa
