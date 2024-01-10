import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args["JOB_NAME"], args)

sourceNode = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://smedia-data-raw-dev/google/abbotsfordnissan_8610421098/"], "recurse": True},
    transformation_ctx="sourceNode",
)

targetNode = glueContext.write_dynamic_frame.from_options(
    frame=sourceNode,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://smedia-data-processing-dev/google/jf_cron_copy_local/",
        "partitionKeys": [],
    },
    transformation_ctx="targetNode",
)

# job.commit()
