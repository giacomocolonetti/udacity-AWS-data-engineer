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
AccelerometerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://stedi-c0bfe335-dd6f-4465-9a94-d665d851c44e-dl/accelerometer/trusted/"
        ],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1679081686035 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [
            "s3://stedi-c0bfe335-dd6f-4465-9a94-d665d851c44e-dl/step_trainer/trusted"
        ],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1679081686035",
)

# Script generated for node Join Tables
JoinTables_node2 = Join.apply(
    frame1=AccelerometerTrusted_node1,
    frame2=StepTrainerTrusted_node1679081686035,
    keys1=["timeStamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="JoinTables_node2",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1679083343450 = glueContext.write_dynamic_frame.from_options(
    frame=JoinTables_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-c0bfe335-dd6f-4465-9a94-d665d851c44e-dl/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node1679083343450",
)

job.commit()
