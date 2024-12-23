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

# COMMAND

#Convert Spark DataFrame to Pandas DataFrame
import pandas as pd
df_geo = df_geo.toPandas()
df_pin = df_pin.toPandas()
df_user = df_user.toPandas()

# COMMAND

# Remove duplicates
df_pin.drop_duplicates(subset='index', keep='first', inplace=True)
df_geo.drop_duplicates(subset='ind', keep='first', inplace=True)
df_user.drop_duplicates(subset='ind', keep='first', inplace=True)

# COMMAND

# Clean df_pin
# Replace empty entries and entries with no relevant data in each column with Nones
import numpy as np
df_pin.replace('', np.nan, inplace=True)
df_pin.replace('None', np.nan, inplace=True)
df_pin.replace('Null', np.nan, inplace=True)
df_pin.replace('null', np.nan, inplace=True)
df_pin.replace('No Title Data Available', np.nan, inplace=True)
df_pin.replace('User Info Error', np.nan, inplace=True)
df_pin.replace('Image src error.', np.nan, inplace=True)

#COMMAND

# Perform the necessary transformations on the follower_count to ensure every entry is a number. Make sure the data type of this column is an int.
# Convert k/K and m/M
df_pin['follower_count'] = df_pin['follower_count'].str.replace('k', '000')
df_pin['follower_count'] = df_pin['follower_count'].str.replace('K', '000')
df_pin['follower_count'] = df_pin['follower_count'].str.replace('m', '000000')
df_pin['follower_count'] = df_pin['follower_count'].str.replace('M', '000000')
# Convert to numeric
df_pin['follower_count'] = pd.to_numeric(df_pin['follower_count'], errors='coerce')
# Convert to int
df_pin['follower_count'] = df_pin['follower_count'].fillna(0).astype(int)

# COMMAND

# Ensure that each column containing numeric data has a numeric data type
df_pin['index'] = pd.to_numeric(df_pin['index'])

# COMMAND

# Clean the data in the save_location column to include only the save location path
df_pin["save_location"] = df_pin["save_location"].str.replace("Local save in ", "")

# COMMAND

# Rename the index column to ind.
df_pin.rename(columns={'index': 'ind'}, inplace=True)

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
# Create a new column coordinates that contains an array based on the latitude and longitude columns
df_geo['coordinates'] = df_geo[['latitude', 'longitude']].values.tolist()

# COMMAND

# Drop the latitude and longitude columns from the DataFrame
df_geo = df_geo.drop(['latitude', 'longitude'], axis=1)

# COMMAND

# Convert the timestamp column from a string to a timestamp data type
df_geo['timestamp'] = pd.to_datetime(df_geo['timestamp'])

# COMMAND

# Reorder the DataFrame columns to have the following column order:
# ind
# country
# coordinates
# timestamp
df_geo = df_geo[['ind', 'country', 'coordinates', 'timestamp']]

# COMMAND

# Clean df_user
# Create a new column user_name that concatenates the information found in the first_name and last_name columns
df_user['user_name'] = df_user['first_name'] + ' ' + df_user['last_name']

# Drop the first_name and last_name columns from the DataFrame
df_user = df_user.drop(['first_name', 'last_name'], axis=1)

# Convert the date_joined column from a string to a timestamp data type
df_user['date_joined'] = pd.to_datetime(df_user['date_joined'])
# COMMAND

# Reorder the DataFrame columns to have the following column order:
# ind
# user_name
# age
# date_joined
df_user = df_user[['ind', 'user_name', 'age', 'date_joined']]

# COMMAND

# Find the most popular Pinterest category people post to based on their country.
# Your query should return a DataFrame that contains the following columns:
# country
# category
# category_count, a new column containing the desired query output
df_cat_count = df_pin.merge(df_geo, left_on='ind', right_on='ind')
df_cat_count = df_cat_count.groupby(['country', 'category']).size().reset_index(name='category_count')
df_cat_count = df_cat_count.sort_values(by='category_count', ascending=False)
df_cat_count.drop_duplicates(subset='country', keep='first', inplace=True)
print(df_cat_count)

# COMMAND

# Find the most popular Pinterest category people post to based on their country.
# Your query should return a DataFrame that contains the following columns:
# country
# category
# category_count, a new column containing the desired query output
df_cat_count = df_pin.merge(df_geo, left_on='ind', right_on='ind')
df_cat_count = df_cat_count.groupby(['country', 'category']).size().reset_index(name='category_count')
df_cat_count = df_cat_count.sort_values(by='category_count', ascending=False)
df_cat_count.drop_duplicates(subset='country', keep='first', inplace=True)
print(df_cat_count)

# COMMAND

# Find out what was the most populat category each year
# Find how many posts each category had between 2018 and 2022.
# Your query should return a DataFrame that contains the following columns:
# post_year, a new column that contains only the year from the timestamp column
# category
# category_count, a new column containing the desired query output
df_cat_count_2 = df_pin.merge(df_geo, left_on='ind', right_on='ind')
df_cat_count_2['post_year'] = df_cat_count_2['timestamp'].dt.year
df_cat_count_2 = df_cat_count_2.groupby(['post_year', 'category']).size().reset_index(name='category_count')
df_cat_count_2 = df_cat_count_2[df_cat_count_2['post_year'].between(2018, 2022)]
df_cat_count_2 = df_cat_count_2.sort_values(by=['post_year', 'category_count'], ascending=[True, False])
# Remove hash from next line if you want just the top category for each year
#df_cat_count_2 = df_cat_count_2.drop_duplicates(subset='post_year', keep='first')
print(df_cat_count_2)

# COMMAND

# Find the user with the most followers in each country
# Step 1: For each country find the user with the most followers.
# Your query should return a DataFrame that contains the following columns:
# country
# poster_name
# follower_count
df_user_follower = df_pin.merge(df_geo, left_on='ind', right_on='ind')
df_user_follower = df_user_follower.groupby(['country']).apply(lambda x: x.nlargest(1, 'follower_count')).reset_index(drop=True)
df_user_follower = df_user_follower[['country', 'poster_name', 'follower_count']]
print(df_user_follower)

# COMMAND

# Step 2: Based on the above query, find the country with the user with most followers.
# Your query should return a DataFrame that contains the following columns:
# country
# follower_count
# This DataFrame should have only one entry.
df_user_follower = df_user_follower.sort_values(by='follower_count', ascending=False)
df_user_follower_country = df_user_follower.head(1)
print(df_user_follower_country)

# COMMAND

# Find the most popular category for different age groups
# What is the most popular category people post to based on the following age groups:
# 18-24
# 25-35
# 36-50
# +50
# Your query should return a DataFrame that contains the following columns:
# age_group, a new column based on the original age column
# category
# category_count, a new column containing the desired query output
df_age_cat = df_pin.merge(df_user, left_on='ind', right_on='ind')
df_age_cat['age_group'] = pd.cut(df_age_cat['age'], bins=[18, 24, 35, 50, 100], labels=['18-24', '25-35', '36-50', '+50'])
df_age_cat = df_age_cat.groupby(['age_group', 'category']).size().reset_index(name='category_count')
df_age_cat = df_age_cat.sort_values(by='category_count', ascending=False)
df_age_cat = df_age_cat.groupby('age_group').head(1)
print(df_age_cat)

# COMMAND

# Find the median follower count for different age groups
# What is the median follower count for users in the following age groups:
# 18-24
# 25-35
# 36-50
# +50
# Your query should return a DataFrame that contains the following columns:
# age_group, a new column based on the original age column
# median_follower_count, a new column containing the desired query output
df_age_med_fol = df_pin.merge(df_user, left_on='ind', right_on='ind')
df_age_med_fol['age_group'] = pd.cut(df_age_med_fol['age'], bins=[18, 24, 35, 50, 100], labels=['18-24', '25-35', '36-50', '+50'])
df_age_med_fol = df_age_med_fol.groupby(['age_group']).agg({'follower_count': 'median'})
df_age_med_fol['follower_count'] = df_age_med_fol['follower_count'].astype(int)
print(df_age_med_fol)

# COMMAND

# Find the median follower count for different age groups
# What is the median follower count for users in the following age groups:
# 18-24
# 25-35
# 36-50
# +50
# Your query should return a DataFrame that contains the following columns:
# age_group, a new column based on the original age column
# median_follower_count, a new column containing the desired query output
df_age_med_fol = df_pin.merge(df_user, left_on='ind', right_on='ind')
df_age_med_fol['age_group'] = pd.cut(df_age_med_fol['age'], bins=[18, 24, 35, 50, 100], labels=['18-24', '25-35', '36-50', '+50'])
df_age_med_fol = df_age_med_fol.groupby(['age_group']).agg({'follower_count': 'median'})
df_age_med_fol['follower_count'] = df_age_med_fol['follower_count'].astype(int)
print(df_age_med_fol)

# COMMAND

# Find out how many users have joined each year
# Find how many users have joined between 2015 and 2020.
# Your query should return a DataFrame that contains the following columns:
# post_year, a new column that contains only the year from the timestamp column
# number_users_joined, a new column containing the desired query output
df_yr_joined = df_geo.merge(df_user, left_on='ind', right_on='ind')
df_yr_joined['post_year'] = df_geo['timestamp'].dt.year
df_yr_joined = df_yr_joined.groupby('post_year').size().reset_index(name='number_users_joined')
df_yr_joined = df_yr_joined[['post_year', 'number_users_joined']]
df_yr_joined = df_yr_joined.sort_values(by='post_year')
df_yr_joined['post_year'] = df_yr_joined['post_year'].astype(int)
df_yr_joined = df_yr_joined.loc[df_yr_joined['post_year'].between(2015, 2020)]
print(df_yr_joined)

# COMMAND

# Find the median follower count of users based on their joining year
# Find the median follower count of users have joined between 2015 and 2020.
# Your query should return a DataFrame that contains the following columns:
# post_year, a new column that contains only the year from the timestamp column
# median_follower_count, a new column containing the desired query output
df_yr_med_fol = df_geo.merge(df_user, left_on='ind', right_on='ind')
df_yr_med_fol = df_yr_med_fol.merge(df_pin, left_on='ind', right_on='ind')
df_yr_med_fol['post_year'] = df_yr_med_fol['timestamp'].dt.year
df_yr_med_fol = df_yr_med_fol.groupby('post_year').agg({'follower_count': 'median'}).reset_index()
df_yr_med_fol['follower_count'] = df_yr_med_fol['follower_count'].astype(int)
df_yr_med_fol.rename(columns={'follower_count': 'median_follower_count'}, inplace=True)
df_yr_med_fol = df_yr_med_fol.loc[df_yr_med_fol['post_year'].between(2015, 2020)]
df_yr_med_fol = df_yr_med_fol[['post_year', 'median_follower_count']]
print(df_yr_med_fol)

# COMMAND

# Find the median follower count of users based on their joining year and age group
# Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.
# Your query should return a DataFrame that contains the following columns:
# age_group, a new column based on the original age column
# post_year, a new column that contains only the year from the timestamp column
# median_follower_count, a new column containing the desired query output
df_age_yr_med_fol = df_geo.merge(df_user, left_on='ind', right_on='ind')
df_age_yr_med_fol = df_age_yr_med_fol.merge(df_pin, left_on='ind', right_on='ind')
df_age_yr_med_fol['age_group'] = pd.cut(df_age_yr_med_fol['age'], bins=[18, 24, 35, 50, 100], labels=['18-24', '25-35', '36-50', '+50'])
df_age_yr_med_fol['post_year'] = df_age_yr_med_fol['timestamp'].dt.year
df_age_yr_med_fol = df_age_yr_med_fol.groupby(['age_group', 'post_year']).agg({'follower_count': 'median'}).reset_index()
df_age_yr_med_fol = df_age_yr_med_fol[['age_group', 'post_year', 'follower_count']]
df_age_yr_med_fol = df_age_yr_med_fol.loc[df_age_yr_med_fol['post_year'].between(2015, 2020)]
df_age_yr_med_fol['follower_count'] = df_age_yr_med_fol['follower_count'].astype(int)
print(df_age_yr_med_fol)