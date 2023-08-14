import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer trusted
accelerometertrusted_node1684740787398 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse/project/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node1684740787398",
)

# Script generated for node customer trusted
customertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse/project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1",
)

# Script generated for node Join
Join_node1684740790418 = Join.apply(
    frame1=accelerometertrusted_node1684740787398,
    frame2=customertrusted_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1684740790418",
)

# Script generated for node Drop Fields
DropFields_node1684741003725 = DropFields.apply(
    frame=Join_node1684740790418,
    paths=["x", "y", "user", "timeStamp", "z"],
    transformation_ctx="DropFields_node1684741003725",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1684743384841 = DynamicFrame.fromDF(
    DropFields_node1684741003725.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1684743384841",
)

# Script generated for node customer curated
customercurated_node1684741239972 = glueContext.getSink(
    path="s3://stedi-lakehouse/project/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customercurated_node1684741239972",
)
customercurated_node1684741239972.setCatalogInfo(
    catalogDatabase="stedi-lakehouse", catalogTableName="customer_curated"
)
customercurated_node1684741239972.setFormat("json")
customercurated_node1684741239972.writeFrame(DropDuplicates_node1684743384841)
job.commit()
