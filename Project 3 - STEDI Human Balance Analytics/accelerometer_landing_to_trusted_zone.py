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

# Script generated for node Customer Landing
CustomerLanding_node1678913757156 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://stedi-c0bfe335-dd6f-4465-9a94-d665d851c44e-dl/customer/landing/"
        ],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1678913757156",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://stedi-c0bfe335-dd6f-4465-9a94-d665d851c44e-dl/accelerometer/landing/"
        ],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1",
)

# Script generated for node Join Customer
JoinCustomer_node2 = Join.apply(
    frame1=AccelerometerLanding_node1,
    frame2=CustomerLanding_node1678913757156,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node2",
)

# Script generated for node Remove Customer Columns
RemoveCustomerColumns_node1678913878879 = DropFields.apply(
    frame=JoinCustomer_node2,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="RemoveCustomerColumns_node1678913878879",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=RemoveCustomerColumns_node1678913878879,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-c0bfe335-dd6f-4465-9a94-d665d851c44e-dl/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node3",
)

job.commit()
