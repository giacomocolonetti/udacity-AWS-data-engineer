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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1679076841705 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://stedi-c0bfe335-dd6f-4465-9a94-d665d851c44e-dl/accelerometer/trusted/"
        ],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1679076841705",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://stedi-c0bfe335-dd6f-4465-9a94-d665d851c44e-dl/customer/trusted/"
        ],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Join Privacy Filter
JoinPrivacyFilter_node2 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerTrusted_node1679076841705,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinPrivacyFilter_node2",
)

# Script generated for node Drop Accelerometer Fields
DropAccelerometerFields_node1679077077443 = DropFields.apply(
    frame=JoinPrivacyFilter_node2,
    paths=[
        "shareWithFriendsAsOfDate",
        "phone",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "serialNumber",
    ],
    transformation_ctx="DropAccelerometerFields_node1679077077443",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropAccelerometerFields_node1679077077443,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-c0bfe335-dd6f-4465-9a94-d665d851c44e-dl/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
