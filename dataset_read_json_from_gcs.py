# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql import SparkSession

# create a spark session
spark = SparkSession.builder.appName("myapp").getOrCreate()

# uniquely identify the input file of interest on GCS
# these files are generated via a cloud storage subscription to a pub/sub topic
# need to adhere to its naming convention detailed here:
# https://cloud.google.com/pubsub/docs/create-cloudstorage-subscription#file_names
bucket: str = "YOUR_BUCKET"
prefix: str = "YOUR_FILE_PREFIX"
Y: str = "2023"
M: str = "09"
D: str = "27"
hh: str = "22"
mm: str = "35"
ss: str = "15"
timezone: str = "00:00" #this is always fixed to UTC
uid: str = "e74865"
suffix = ".jsonl"

# use the spark session methods to read in the json file as a PySpark Dataframe
df1 = (
    spark
    .read
    .json(f"gs://{bucket}/{prefix}{Y}-{M}-{D}T{hh}:{mm}:{ss}+{timezone}_{uid}{suffix}")
)

# Transform the Dataframe
df2 = df1.filter(df1['isInStock'] == True)

# Write out the transformed Dataframe to GCS
# uniquely identify the output directory on GCS
# PySpark will create multiple JSON files underneath this directory on GCS
bucket: str = "YOUR_BUCKET"
gcsdirectory: str = f"gs://{bucket}/instockorders/{Y}/{M}/{D}/{hh}"
df2.write.json(gcsdirectory)