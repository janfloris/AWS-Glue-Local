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

# Script generated for node Amazon S3
sourceNode = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://smedia-data-raw-dev/google/"],
        "recurse": True,
    },
    transformation_ctx="sourceNode",
)

# Script generated for node Data Change
transformNode = ApplyMapping.apply(
    frame=sourceNode,
    mappings=[
        ("resourcename", "string", "resname", "string"),
        ("status", "string", "status", "string"),
        ("basecampaign", "string", "basecampaign", "string"),
        ("name", "string", "name", "string"),
        ("id", "string", "id", "string"),
        ("campaignbudget", "string", "campaignbudget", "string"),
        ("startdate", "string", "startdate", "string"),
        ("enddate", "string", "enddate", "string"),
        (
            "adservingoptimizationstatus",
            "string",
            "adservingoptimizationstatus",
            "string",
        ),
        ("advertisingchanneltype", "string", "advertisingchanneltype", "string"),
        ("advertisingchannelsubtype", "string", "advertisingchannelsubtype", "string"),
        ("experimenttype", "string", "experimenttype", "string"),
        ("servingstatus", "string", "servingstatus", "string"),
        ("biddingstrategytype", "string", "biddingstrategytype", "string"),
        ("domainname", "string", "domainname", "string"),
        ("languagecode", "string", "languagecode", "string"),
        ("usesuppliedurlsonly", "string", "usesuppliedurlsonly", "string"),
        ("positivegeotargettype", "string", "positivegeotargettype", "string"),
        ("negativegeotargettype", "string", "negativegeotargettype", "string"),
        ("paymentmode", "string", "paymentmode", "string"),
        ("optimizationgoaltypes", "string", "optimizationgoaltypes", "string"),
        ("date", "string", "date", "string"),
        ("averagecost", "string", "averagecost", "string"),
        ("clicks", "string", "clicks", "string"),
        ("costmicros", "string", "costmicros", "string"),
        ("impressions", "string", "impressions", "string"),
        ("useaudiencegrouped", "string", "useaudiencegrouped", "string"),
        (
            "activeviewmeasurablecostmicros",
            "string",
            "activeviewmeasurablecostmicros",
            "string",
        ),
        ("costperallconversions", "string", "costperallconversions", "string"),
        ("costperconversion", "string", "costperconversion", "string"),
        ("invalidclicks", "string", "invalidclicks", "string"),
        ("publisherpurchasedclicks", "string", "publisherpurchasedclicks", "string"),
        ("averagepageviews", "string", "averagepageviews", "string"),
        ("videoviews", "string", "videoviews", "string"),
        (
            "allconversionsbyconversiondate",
            "string",
            "allconversionsbyconversiondate",
            "string",
        ),
        (
            "allconversionsvaluebyconversiondate",
            "string",
            "allconversionsvaluebyconversiondate",
            "string",
        ),
        (
            "conversionsbyconversiondate",
            "string",
            "conversionsbyconversiondate",
            "string",
        ),
        (
            "conversionsvaluebyconversiondate",
            "string",
            "conversionsvaluebyconversiondate",
            "string",
        ),
        (
            "valueperallconversionsbyconversiondate",
            "string",
            "valueperallconversionsbyconversiondate",
            "string",
        ),
        (
            "valueperconversionsbyconversiondate",
            "string",
            "valueperconversionsbyconversiondate",
            "string",
        ),
        ("allconversions", "string", "allconversions", "string"),
        (
            "absolutetopimpressionpercentage",
            "string",
            "absolutetopimpressionpercentage",
            "string",
        ),
        (
            "searchabsolutetopimpressionshare",
            "string",
            "searchabsolutetopimpressionshare",
            "string",
        ),
        ("averagecpc", "string", "averagecpc", "string"),
        ("searchimpressionshare", "string", "searchimpressionshare", "string"),
        ("searchtopimpressionshare", "string", "searchtopimpressionshare", "string"),
        ("activeviewctr", "string", "activeviewctr", "string"),
        ("ctr", "string", "ctr", "string"),
    ],
    transformation_ctx="transformNode",
)

# Script generated for node Amazon S3
targetNode = glueContext.write_dynamic_frame.from_options(
    frame=transformNode,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://smedia-data-processing-dev/google/jf_cron_transf_local/",
        "partitionKeys": [],
    },
    transformation_ctx="targetNode",
)

# job.commit()
