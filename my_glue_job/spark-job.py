import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load data from S3 into a Glue DynamicFrame
data_source = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://airbnb-data-workshop/csv_data/raw_listings.csv"], "recurse": True},
    transformation_ctx="data_source"
)

# Change the schema of the DynamicFrame to align with Spark DataFrame expectations
data_transform = ApplyMapping.apply(
    frame=data_source,
    mappings=[
        ("id", "string", "listing_id", "string"),
        ("listing_url", "string", "listing_url", "string"),
        ("name", "string", "listing_name", "string"),
        ("room_type", "string", "room_type", "string"),
        ("minimum_nights", "string", "minimum_nights", "string"),
        ("host_id", "string", "host_id", "string"),
        ("price", "string", "price_str", "string"),
        ("created_at", "string", "created_at", "string"),
        ("updated_at", "string", "updated_at", "string")
    ],
    transformation_ctx="data_transform"
)

# Convert the DynamicFrame to a Spark DataFrame
csv_df = data_transform.toDF()

# Apply the transformations
transformed_df = csv_df.select(
    F.col("listing_id"),
    F.col("listing_name"),
    F.col("room_type"),
    F.when(F.col("minimum_nights") == "0", "1").otherwise(F.col("minimum_nights")).alias("minimum_nights"),
    F.col("host_id"),
    F.regexp_replace(F.col("price_str"), "\\$", "").cast("double").alias("price"),
    F.to_timestamp(F.regexp_replace(F.regexp_replace(F.col("created_at"), "T", " "), "Z", ""), "yyyy-MM-dd HH:mm:ss").alias("created_at"),
    F.to_timestamp(F.regexp_replace(F.regexp_replace(F.col("updated_at"), "T", " "), "Z", ""), "yyyy-MM-dd HH:mm:ss").alias("updated_at")  # Updated transformation for updated_at
)

# Convert the transformed Spark DataFrame back to a Glue DynamicFrame
transformed_dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "transformed_dynamic_frame")

# Write the transformed DynamicFrame to S3
data_target = glueContext.getSink(
    path="s3://airbnb-data-workshop/glue_transformed_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="data_target"
)
data_target.setCatalogInfo(catalogDatabase="workshop_db", catalogTableName="transformed_listings")
data_target.setFormat("glueparquet", compression="uncompressed")
data_target.writeFrame(transformed_dynamic_frame)

# Commit the job
job.commit()