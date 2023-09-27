from pyspark.sql import SparkSession

#    spark = SparkSession \
#         .builder \
#         .appName("Python Spark SQL basic example") \
#         .config("spark.driver.extraClassPath", "/path/to/jdbc/driver/postgresql-42.1.4.jar") \
#         .getOrCreate()

spark = SparkSession.builder.appName("myapp").getOrCreate()

dbname = "guestbook"
connection="cloudSqlInstance=dataproc-demo-400309:europe-west2:cloudsql-instance"
socketfactory="socketFactory=com.google.cloud.sql.mysql.SocketFactory"
user="user=alazzaro"
password="password={_F/y_73Q,uz/j1f"
url=f"jdbc:mysql:///{dbname}?{connection}&{socketfactory}&{user}&{password}"

df1 = (
    spark
    .read
    .format("jdbc")
    .option("url",url)
    .option("dbtable", "schema.entries")
    .option("user", "alazzaro")
    .option("password", "{_F/y_73Q,uz/j1f")
    .load()
)

df1.show()


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("myapp").getOrCreate()

dbname = "guestbook"
connection="cloudSqlInstance=dataproc-demo-400309:europe-west2:cloudsql-instance"
socketfactory="socketFactory=com.google.cloud.sql.mysql.SocketFactory"
user="user=abc"
password="password=def"
url=f"jdbc:mysql:///{dbname}?{connection}&{socketfactory}&{user}&{password}"

df1 = (
    spark
    .read
    .format("jdbc")
    .option("url",url)
    .option("dbtable", "schema.entries")
    .load()
)

df1.show()