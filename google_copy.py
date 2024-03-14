import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

sourceNode = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://media-data-raw-dev/google/"], "recurse": True},
    transformation_ctx="sourceNode",
)

targetNode = glueContext.write_dynamic_frame.from_options(
    frame=sourceNode,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://media-data-processing-dev/google/jf_cron_copy_local/",
        "partitionKeys": [],
    },
    transformation_ctx="targetNode",
)
