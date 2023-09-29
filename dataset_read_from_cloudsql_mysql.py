from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("myapp").getOrCreate()

format = "jdbc"
dbtype = "mysql"
database_name = "guestbook"
instance_connection_name="cloudSqlInstance=dataproc-demo-400309:europe-west2:cloudsql-instance"
cloudsql_connector="socketFactory=com.google.cloud.sql.mysql.SocketFactory"
user="alazzaro"
password="{_F/y_73Q,uz/j1f"
url=f"{format}:{dbtype}:///{database_name}?{instance_connection_name}&{cloudsql_connector}&user={user}&password={password}"

df1 = (
    spark
    .read
    .format(format)
    .option("url",url)
    .option("query", "select * from entries;")
    .load()
)

df1.show()