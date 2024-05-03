#IAM-Role:- sae-lambda-glue-access 
import sys
import time
import json
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import *
import logging
import psycopg2
import boto3

# Set up logger for printing to logs
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Function to get Redshift secret
def get_secret():
    secret_name = "data-warehouse-dev-secret-manager"
    region_name = "us-west-2"

    # Create a Secrets Manager client
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # Handle exceptions as needed for your application
        print(f"Error: {e}")
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

# Get Redshift secret
redshift_secret = get_secret()

# Redshift connection options
s3_temp_dir = 's3://sae-validation-poc/red-temp-dir/'

# Define the schema for the result DataFrame
schema = StructType([
    StructField("Table_Name", StringType(), True),
    StructField("Row_Count", IntegerType(), True),
])

# Create an empty DataFrame to collect results
result_df = spark.createDataFrame([], schema)

# Function to execute SQL query on Redshift
def execute_redshift_query(query):
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=redshift_secret["dbname"],
            user=redshift_secret["username"],
            password=redshift_secret["password"],
            host=redshift_secret["host"],
            port=redshift_secret["port"]
        )
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"Error executing Redshift query: {e}")
    finally:
        if conn is not None:
            conn.close()

# SQL query to extract table_name and row_count
sql_query = """
SELECT c.relname AS table_name, c.reltuples AS row_count
FROM pg_namespace n
JOIN pg_class c ON n.oid = c.relnamespace
JOIN pg_attribute a ON c.oid = a.attrelid
WHERE n.nspname LIKE 'stg_sae_sp_che%'
AND n.nspname NOT LIKE '%_hist'
AND n.nspname != 'information_schema'
AND a.attnum > 0
GROUP BY table_name, c.reltuples
ORDER BY table_name
"""

# Execute SQL query to get table information
table_info = execute_redshift_query(sql_query)

# Iterate through the table information and populate the result DataFrame
for row in table_info:
    result_df = result_df.union(spark.createDataFrame([row], schema))

# Show the DataFrame
result_df.show()

# Close the SparkContext
sc.stop()

# Job commit to finish the Glue job
job.commit()

