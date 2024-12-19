# COMMAND

# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# COMMAND

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "s3://user-1254dc635ec5-bucket/topics/1254dc635ec5.geo/partition=0/*.json" 
# ^^^ HERE: --> User has full access to read any buckets under the AWS sandbox we use, so you don't have to verify with the access keys from before
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
geo_df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(geo_df)

# COMMAND

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "s3://user-1254dc635ec5-bucket/topics/1254dc635ec5.geo/partition=0/*.json" 
# ^^^ HERE: --> User has full access to read any buckets under the AWS sandbox we use, so you don't have to verify with the access keys from before
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
geo_df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(geo_df)

# COMMAND

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "s3://user-1254dc635ec5-bucket/topics/1254dc635ec5.geo/partition=0/*.json" 
# ^^^ HERE: --> User has full access to read any buckets under the AWS sandbox we use, so you don't have to verify with the access keys from before
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
geo_df = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(geo_df)