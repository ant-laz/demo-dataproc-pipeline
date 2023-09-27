from pyspark.sql import SparkSession

# create a spark session
spark = SparkSession.builder.appName("myapp").getOrCreate()

# uniquely identify the input file of interest on GCS
# these files are generated via a cloud storage subscription to a pub/sub topic
# need to adhere to its naming convention detailed here:
# https://cloud.google.com/pubsub/docs/create-cloudstorage-subscription#file_names
bucket: str = "python-lab-329118-demo-dataproc-pipeline"
prefix: str = "shikharorder"
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
bucket: str = "python-lab-329118-demo-dataproc-pipeline"
gcsdirectory: str = f"gs://{bucket}/instockorders/{Y}/{M}/{D}/{hh}"
df2.write.json(gcsdirectory)