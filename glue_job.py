import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node daily_raw_flight_data_from_s3
daily_raw_flight_data_from_s3_node1709567870832 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="airline_db",
        table_name="daily_raw_data",
        transformation_ctx="daily_raw_flight_data_from_s3_node1709567870832",
    )
)

# Script generated for node dim_airport_data_redshift
dim_airport_data_redshift_node1709567971580 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="airline_db",
        table_name="dev_airlines_airports_dim",
        redshift_tmp_dir="s3://redshift-query-temp-bucket",
        transformation_ctx="dim_airport_data_redshift_node1709567971580",
    )
)

# Script generated for node Join
Join_node1709568405876 = Join.apply(
    frame1=daily_raw_flight_data_from_s3_node1709567870832,
    frame2=dim_airport_data_redshift_node1709567971580,
    keys1=["originairportid"],
    keys2=["airport_id"],
    transformation_ctx="Join_node1709568405876",
)

# Script generated for node dept_airport_schema
dept_airport_schema_node1709568482045 = ApplyMapping.apply(
    frame=Join_node1709568405876,
    mappings=[
        ("carrier", "string", "carrier", "string"),
        ("destairportid", "long", "destairportid", "long"),
        ("depdelay", "long", "dep_delay", "bigint"),
        ("arrdelay", "long", "arr_delay", "bigint"),
        ("city", "string", "dep_city", "string"),
        ("name", "string", "dep_airport", "string"),
        ("state", "string", "dep_state", "string"),
    ],
    transformation_ctx="dept_airport_schema_node1709568482045",
)

# Script generated for node Join
Join_node1709569040712 = Join.apply(
    frame1=dept_airport_schema_node1709568482045,
    frame2=dim_airport_data_redshift_node1709567971580,
    keys1=["destairportid"],
    keys2=["airport_id"],
    transformation_ctx="Join_node1709569040712",
)

# Script generated for node Change Schema
ChangeSchema_node1709569126441 = ApplyMapping.apply(
    frame=Join_node1709569040712,
    mappings=[
        ("carrier", "string", "carrier", "string"),
        ("dep_delay", "bigint", "dep_delay", "bigint"),
        ("arr_delay", "bigint", "arr_delay", "bigint"),
        ("dep_city", "string", "dep_city", "string"),
        ("dep_airport", "string", "dep_airport", "string"),
        ("dep_state", "string", "dep_state", "string"),
        ("city", "string", "arr_city", "string"),
        ("name", "string", "arr_airport", "string"),
        ("state", "string", "arr_state", "string"),
    ],
    transformation_ctx="ChangeSchema_node1709569126441",
)

# Script generated for node redshift_fact_table_write
redshift_fact_table_write_node1709569357507 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1709569126441,
    connection_type="redshift",
    connection_options={
        "useConnectionProperties": "true",
        "dbtable": "airlines.daily_flights_fact",
        "connectionName": "Redshift connection",
        "redshiftTmpDir": "s3://redshift-query-temp-bucket",
        "preactions": "CREATE TABLE IF NOT EXISTS airlines.daily_flights_fact (carrier VARCHAR, dep_delay VARCHAR, arr_delay VARCHAR, dep_city VARCHAR, dep_airport VARCHAR, dep_state VARCHAR, arr_city VARCHAR, arr_airport VARCHAR, arr_state VARCHAR);",
    },
    transformation_ctx="redshift_fact_table_write_node1709569357507",
)

job.commit()
