from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("myapp").getOrCreate()

df1 = (
    spark
    .read
    .option("multiline", True)
    .json("gs://python-lab-329118-demo-dataproc-pipeline/test.json")
)