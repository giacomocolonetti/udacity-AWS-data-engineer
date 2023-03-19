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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://stedi-c0bfe335-dd6f-4465-9a94-d665d851c44e-dl/step_trainer/landing/"
        ],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1679078646300 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://stedi-c0bfe335-dd6f-4465-9a94-d665d851c44e-dl/customer/trusted/"
        ],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1679078646300",
)

# Script generated for node Privacy Filter
PrivacyFilter_node2 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerTrusted_node1679078646300,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="PrivacyFilter_node2",
)

# Script generated for node Drop Customer Fields
DropCustomerFields_node3 = DropFields.apply(
    frame=PrivacyFilter_node2,
    paths=[
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropCustomerFields_node3",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1679078643901 = glueContext.write_dynamic_frame.from_options(
    frame=DropCustomerFields_node3,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-c0bfe335-dd6f-4465-9a94-d665d851c44e-dl/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1679078643901",
)

job.commit()
