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

# Script generated for node step_trainer zone
step_trainerzone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://loiln-stedi-lake-house/step_trainer/"],
        "recurse": True,
    },
    transformation_ctx="step_trainerzone_node1",
)

# Script generated for node customer_curated zone
customer_curatedzone_node1683462536708 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://loiln-stedi-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curatedzone_node1683462536708",
)

# Script generated for node Join
Join_node2 = Join.apply(
    frame1=step_trainerzone_node1,
    frame2=customer_curatedzone_node1683462536708,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node2",
)

# Script generated for node step_trainer_trusted zone
step_trainer_trustedzone_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node2,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://loiln-stedi-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trustedzone_node3",
)

job.commit()
