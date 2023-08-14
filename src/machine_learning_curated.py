import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Stept trainer (trusted zone)
Stepttrainertrustedzone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse/project/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Stepttrainertrustedzone_node1",
)

# Script generated for node Accelerometer (trusted)
Accelerometertrusted_node1684756409256 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse/project/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometertrusted_node1684756409256",
)

# Script generated for node Curated customers data
Curatedcustomersdata_node1684756805582 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse/project/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="Curatedcustomersdata_node1684756805582",
)

# Script generated for node Join
Join_node1684756746589 = Join.apply(
    frame1=Accelerometertrusted_node1684756409256,
    frame2=Curatedcustomersdata_node1684756805582,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1684756746589",
)

# Script generated for node Join
Join_node1684756934469 = Join.apply(
    frame1=Join_node1684756746589,
    frame2=Stepttrainertrustedzone_node1,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1684756934469",
)

# Script generated for node Aggregate
Aggregate_node1684757038141 = sparkAggregate(
    glueContext,
    parentFrame=Join_node1684756934469,
    groups=[
        "y",
        "z",
        "x",
        "user",
        "sensorReadingTime",
        "distanceFromObject",
        "serialNumber",
    ],
    aggs=[["timeStamp", "count"]],
    transformation_ctx="Aggregate_node1684757038141",
)

# Script generated for node Machine Learning data (curated)
MachineLearningdatacurated_node1684757979146 = glueContext.getSink(
    path="s3://stedi-lakehouse/project/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningdatacurated_node1684757979146",
)
MachineLearningdatacurated_node1684757979146.setCatalogInfo(
    catalogDatabase="stedi-lakehouse", catalogTableName="machine_learning_curated"
)
MachineLearningdatacurated_node1684757979146.setFormat("json")
MachineLearningdatacurated_node1684757979146.writeFrame(Aggregate_node1684757038141)
job.commit()
