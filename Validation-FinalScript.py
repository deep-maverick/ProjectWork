import sys
import boto3
import json
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from botocore.exceptions import ClientError
import datetime
from pyspark.sql.utils import AnalysisException
import pandas as pd

# Configure logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ssm_dict"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # Initialize AWS SNS Client
    client = boto3.client('sns')

    # Parse the JSON string to a Python dictionary
    ssm_dict = json.loads(args["ssm_dict"])

    # Access the schema_name and SNSPublishTopicArn parameters
    schema_name = ssm_dict.get("schema_name")
    SNSPublishTopicArn = ssm_dict.get("SNSPublishTopicArn")
    table_names_str = ssm_dict.get("table_names", "")

    # Convert the comma-separated string to a list
    table_names = table_names_str.split(",")

    # Create an empty DataFrame to collect results
    result_schema = StructType([
        StructField("Table_Name", StringType(), True),
        StructField("Row_Count", IntegerType(), True),
    ])

    # Calculate the previous day's date
    previous_date = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y%m%d")
    logging.info(f"Previous date: {previous_date}")

    # S3 Integration
    s3_df = spark.createDataFrame([], result_schema)
    tables_with_count_gt_zero = []

    # S3 Integration
    for table_name in table_names:
        try:
            # Read the Parquet file for each table directly using Spark DataFrame
            s3_file_path = f"s3://unfi-data-landing-dev-s3/dev/dms_landing/sae_sp_aur/selectorpro/{table_name}/{previous_date}/*.parquet"
            table_df = spark.read.parquet(s3_file_path)

            # Get the row count for the table
            row_count = table_df.count()

            if row_count > 0:
                # Append the result to the result DataFrame
                s3_df = s3_df.union(spark.createDataFrame([(table_name, row_count)], result_schema))

                # Store table_name with count = 0
                tables_with_count_gt_zero.append(table_name)
        except AnalysisException as e:
            # Handle the case where the Parquet file is empty or doesn't exist
            logging.warning(f"{table_name} doesn't have daily load today: {str(e)}")

    # Redshift Integration
    red_df = spark.createDataFrame([], result_schema)

    try:
        # Loop through the table names
        for table in table_names:
            # Convert table name to lowercase and add "stg_" prefix
            formatted_table_name = f"stg_{table.lower()}"
            query = f'SELECT count(*) as Row_Count FROM "{schema_name}"."{formatted_table_name}"'
            # Create dynamic frame for each table
            dynamic_frame = glueContext.create_dynamic_frame.from_options(
                connection_type="redshift",
                connection_options={
                    "redshiftTmpDir": "s3://aws-glue-assets-513869633192-us-west-2/temporary/",
                    "dbtable": f'"{schema_name}"."{formatted_table_name}"',
                    "useConnectionProperties": "true",
                    "connectionName": "dev-edw-orch-sae-glue-dms-sae_dp_conv-redshift-connection",
                },
                transformation_ctx="dynamic_frame",
            )
            # Get row count from the dynamic frame
            row_count = dynamic_frame.count()
            # Append formatted table name and row count to the result DataFrame
            result_row = spark.createDataFrame([(formatted_table_name, row_count)], result_schema)

            # Remove "stg_" prefix from the table names, make it case-insensitive, and select necessary columns
            result_row = result_row.withColumn("Table_Name", expr("lower(substring(Table_Name, 5))"))

            # Append the result_row DataFrame to red_df
            red_df = red_df.union(result_row)

    except Exception as e:
        # Log the exception
        logging.error(f"Redshift integration failed: {str(e)}")

    # Display the results Redshift TableName and RowCount Results:

    # Validate table_name and row_count between S3 and Redshift
    validation_result = "Validation Result: "
    try:
        s3_df = s3_df.selectExpr("lower(Table_Name) as S3_Table_Name", "Row_Count as S3_Row_Count")
        red_df = red_df.selectExpr("lower(Table_Name) as Red_Table_Name", "Row_Count as Red_Row_Count")

        red_df_filtered = red_df.join(s3_df, col("Red_Table_Name") == col("S3_Table_Name"), "inner") \
            .select("Red_Table_Name", "Red_Row_Count") \
            .orderBy("Red_Table_Name")

        joined_df = s3_df.join(red_df, col("S3_Table_Name") == col("Red_Table_Name"), "inner")
        
        diff_df = joined_df.withColumn("Count_Match", expr("S3_Row_Count = Red_Row_Count"))

        if diff_df.filter("Count_Match = false").count() == 0:
            validation_result += f"for {schema_name} has Matched"
            validation_status = "Success"
        else:
            validation_result += f"for {schema_name} has Not Matched"
            validation_status = "Failed"

    except Exception as e:
        validation_result += f"Validation failed: {str(e)}"

    # Display the validation result
    logging.info(validation_result)

    # Assuming s3_df and red_df are your PySpark DataFrames
    s3_df_pandas = s3_df.toPandas()
    red_df_pandas = red_df_filtered.toPandas()

    # Concatenate Pandas DataFrames
    result_pandas = pd.concat([s3_df_pandas, red_df_pandas], axis=1)

    # Convert result_pandas values into string before publishing in SNS
    result_pandas_str = result_pandas.to_string()

    # Display the concatenated Pandas DataFrame
    logging.info(result_pandas)

    # Publish the data-validation result and send E-mail
    msg = f"""
    Data validation status of schema {schema_name} {validation_status}

    ------------------------------------------------------------------------------------
    Summary of the tables:
    ------------------------------------------------------------------------------------

    {result_pandas_str}
    """

    sub = f"Data validation Status of schema {schema_name} - {validation_status}"
    client.publish(TopicArn=SNSPublishTopicArn, Message=msg, Subject=sub)

    job.commit()

if __name__ == "__main__":
    main()
