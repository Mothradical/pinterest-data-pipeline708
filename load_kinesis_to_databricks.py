# COMMAND

from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND

%sql
-- Disable format checks during the reading of Delta tables
SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND

df = spark \
.readStream \
.format('kinesis') \
.option('streamName','Kinesis-Prod-Stream') \
.option('initialPosition','latest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# COMMAND

# Check to make sure all is well to this point
display(df)

# COMMAND

# Split into three dfs based on the partitionKey
df_pin = df.filter(df.partitionKey == "1254dc635ec5-pin")
df_geo = df.filter(df.partitionKey == "1254dc635ec5-geo")
df_user = df.filter(df.partitionKey == "1254dc635ec5-user")

# COMMAND

# Convert data fron serialised byte format to JSON
df_pin = df_pin.selectExpr("CAST(data as STRING) jsonData")
df_geo = df_geo.selectExpr("CAST(data as STRING) jsonData")
df_user = df_user.selectExpr("CAST(data as STRING) jsonData")

# COMMAND

# Check to make sure json conversion successful
display(df_geo)

# COMMAND

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Define StructTypes
pin_schema_struct = StructType([
    StructField("index", IntegerType(), True),
    StructField("unique_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("poster_name", StringType(), True),
    StructField("follower_count", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("downloaded", StringType(), True),
    StructField("save_location", StringType(), True),
    StructField("category", StringType(), True)
])

geo_schema_struct = StructType([
    StructField("ind", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("country", StringType(), True)
])

user_schema_struct = StructType([
    StructField("ind", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("date_joined", TimestampType(), True)
])

# COMMAND

# Converting JSON data to columns
df_pin = df_pin.select(from_json("jsonData", pin_schema_struct).alias("data")).select("data.*")
df_geo = df_geo.select(from_json("jsonData", geo_schema_struct).alias("data")).select("data.*")
df_user = df_user.select(from_json("jsonData", user_schema_struct).alias("data")).select("data.*")

# COMMAND

# Check to make sure json has converted correctly
display(df_geo)

# COMMAND

# Remove duplicates
df_pin.drop_duplicates(['index'])
df_geo.drop_duplicates(['ind'])
df_user.drop_duplicates(['ind'])

# COMMAND

# Clean df_pin
# Replace empty entries and entries with no relevant data in each column with Nones
df_pin = df_pin.na.replace(
    ['', 'None', 'Null', 'null', 'No Title Data Available', 'User Info Error', 'Image src error.'],
    [None] * 7
)

# COMMAND

# Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.
from pyspark.sql.functions import regexp_replace, col

# Convert k/K and m/M
df_pin = df_pin.withColumn('follower_count', regexp_replace(col('follower_count'), 'k', '000'))
df_pin = df_pin.withColumn('follower_count', regexp_replace(col('follower_count'), 'K', '000'))
df_pin = df_pin.withColumn('follower_count', regexp_replace(col('follower_count'), 'm', '000000'))
df_pin = df_pin.withColumn('follower_count', regexp_replace(col('follower_count'), 'M', '000000'))

# Convert to numeric and then to int
df_pin = df_pin.withColumn('follower_count', col('follower_count').cast('double'))
df_pin = df_pin.fillna({'follower_count': 0})
df_pin = df_pin.withColumn('follower_count', col('follower_count').cast('int'))

# COMMAND

# Clean the data in the save_location column to include only the save location path
df_pin = df_pin.withColumn(
    "save_location",
    regexp_replace("save_location", "Local save in ", "")
)

# COMMAND

# Rename the index column to ind.
df_pin = df_pin.withColumnRenamed('index', 'ind')

# COMMAND

# Reorder the DataFrame columns to have the following column order:
# ind
# unique_id
# title
# description
# follower_count
# poster_name
# tag_list
# is_image_or_video
# image_src
# save_location
# category
df_pin = df_pin[['ind', 'unique_id', 'title', 'description', 'follower_count', 'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 'save_location', 'category']]

# COMMAND

# Clean df_geo
from pyspark.sql.functions import array

# Create a new column coordinates that contains an array based on the latitude and longitude columns
df_geo = df_geo.withColumn('coordinates', array(col('latitude'), col('longitude')))

# COMMAND

# Drop the latitude and longitude columns from the DataFrame
df_geo = df_geo.drop('latitude', 'longitude')

# COMMAND

# Reorder the DataFrame columns to have the following column order:
# ind
# country
# coordinates
# timestamp
df_geo = df_geo[['ind', 'country', 'coordinates', 'timestamp']]

# COMMAND

# Clean df_user
from pyspark.sql.functions import concat_ws

# Create a new column user_name that concatenates the information found in the first_name and last_name columns
df_user = df_user.withColumn('user_name', concat_ws(' ', df_user['first_name'], df_user['last_name']))

# COMMAND

# Drop the first_name and last_name columns from the DataFrame
df_user = df_user.drop('first_name', 'last_name')

# COMMAND

# Reorder the DataFrame columns to have the following column order:
# ind
# user_name
# age
# date_joined
df_user = df_user[['ind', 'user_name', 'age', 'date_joined']]

# COMMAND

# Check that the transformations have been executed correctly
display(df_pin)

# COMMAND

# Check that the transformations have been executed correctly
display(df_geo)

# COMMAND

# Check that the transformations have been executed correctly
display(df_user)

# COMMAND

# Save each DataFrame to a Delta table

df_pin.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints_pin/") \
  .table("1254dc635ec5_pin_table")

df_geo.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints_geo/") \
  .table("1254dc635ec5_geo_table")

df_user.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints_user/") \
  .table("1254dc635ec5_user_table")