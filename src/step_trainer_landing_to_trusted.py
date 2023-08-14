import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3  step trainer (landing zone)
S3steptrainerlandingzone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse/project/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3steptrainerlandingzone_node1",
)

# Script generated for node Curated customer data
Curatedcustomerdata_node1684746478980 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse/project/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="Curatedcustomerdata_node1684746478980",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1684748956559 = ApplyMapping.apply(
    frame=Curatedcustomerdata_node1684746478980,
    mappings=[
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("birthDay", "string", "`(right) birthDay`", "string"),
        (
            "shareWithResearchAsOfDate",
            "long",
            "`(right) shareWithResearchAsOfDate`",
            "long",
        ),
        ("registrationDate", "long", "`(right) registrationDate`", "long"),
        ("customerName", "string", "`(right) customerName`", "string"),
        ("email", "string", "`(right) email`", "string"),
        ("lastUpdateDate", "long", "`(right) lastUpdateDate`", "long"),
        ("phone", "string", "`(right) phone`", "string"),
        (
            "shareWithPublicAsOfDate",
            "long",
            "`(right) shareWithPublicAsOfDate`",
            "long",
        ),
        (
            "shareWithFriendsAsOfDate",
            "long",
            "`(right) shareWithFriendsAsOfDate`",
            "long",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1684748956559",
)

# Script generated for node Join
S3steptrainerlandingzone_node1DF = S3steptrainerlandingzone_node1.toDF()
RenamedkeysforJoin_node1684748956559DF = RenamedkeysforJoin_node1684748956559.toDF()
Join_node1684746473060 = DynamicFrame.fromDF(
    S3steptrainerlandingzone_node1DF.join(
        RenamedkeysforJoin_node1684748956559DF,
        (
            S3steptrainerlandingzone_node1DF["serialNumber"]
            == RenamedkeysforJoin_node1684748956559DF["`(right) serialNumber`"]
        ),
        "leftsemi",
    ),
    glueContext,
    "Join_node1684746473060",
)

# Script generated for node Drop Fields
DropFields_node1684746890790 = DropFields.apply(
    frame=Join_node1684746473060,
    paths=[
        "`(right) serialNumber`",
        "`(right) birthDay`",
        "`(right) shareWithResearchAsOfDate`",
        "`(right) registrationDate`",
        "`(right) customerName`",
        "`(right) email`",
        "`(right) lastUpdateDate`",
        "`(right) phone`",
        "`(right) shareWithPublicAsOfDate`",
        "`(right) shareWithFriendsAsOfDate`",
    ],
    transformation_ctx="DropFields_node1684746890790",
)

# Script generated for node Trusted Trainer Records
TrustedTrainerRecords_node1684746928882 = glueContext.getSink(
    path="s3://stedi-lakehouse/project/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedTrainerRecords_node1684746928882",
)
TrustedTrainerRecords_node1684746928882.setCatalogInfo(
    catalogDatabase="stedi-lakehouse", catalogTableName="step_trainer_trusted"
)
TrustedTrainerRecords_node1684746928882.setFormat("json")
TrustedTrainerRecords_node1684746928882.writeFrame(DropFields_node1684746890790)
job.commit()
