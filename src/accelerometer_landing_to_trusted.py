import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Landing Accelerometer data
LandingAccelerometerdata_node1684513388030 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stedi-lakehouse/project/accelerometer/landing/"],
            "recurse": True,
        },
        transformation_ctx="LandingAccelerometerdata_node1684513388030",
    )
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse/project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1",
)

# Script generated for node Join Customer
JoinCustomer_node1684513375647 = Join.apply(
    frame1=LandingAccelerometerdata_node1684513388030,
    frame2=CustomerTrustedZone_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1684513375647",
)

# Script generated for node Drop Fields
DropFields_node1684513639194 = DropFields.apply(
    frame=JoinCustomer_node1684513375647,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthday",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "lastUpdateDate",
        "email",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1684513639194",
)

# Script generated for node Trusted Accelerometer
TrustedAccelerometer_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1684513639194,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse/project/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="TrustedAccelerometer_node3",
)

job.commit()
