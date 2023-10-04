# options for spark.read.format(jdbc) from here:
# https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
# The name of the class that implements java.sql.Driver 
# in MySQL Connector/J is com.mysql.cj.jdbc.Driver.
# https://dev.mysql.com/doc/connector-j/8.1/en/connector-j-reference-driver-name.html

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("myapp").getOrCreate()

# format = "jdbc"
# dbtype = "mysql"
# database_name = "guestbook"
# instance_connection_name="cloudSqlInstance=dataproc-demo-400309:europe-west2:cloudsql-instance"
# cloudsql_connector="socketFactory=com.google.cloud.sql.mysql.SocketFactory"
# user="alazzaro"
# password="{_F/y_73Q,uz/j1f"
# url=f"{format}:{dbtype}:///{database_name}?{instance_connection_name}&{cloudsql_connector}&user={user}&password={password}"

# df1 = (
#     spark
#     .read
#     .format("jdbc")
#     .option("url","jdbc:mysql://localhost:3037/guestbook&user=alazzaro&password={_F/y_73Q,uz/j1f&cloudSqlInstance=dataproc-demo-400309:europe-west2:cloudsql-instance")
#     .option("driver","com.mysql.cj.jdbc.Driver")
#     .option("query", "select * from entries;")
#     .load()
# )

# df1 = (
#     spark
#     .read
#     .format("jdbc")
#     .option("url","jdbc:mysql://localhost:3036/guestbook?user=alazzaro&password={_F/y_73Q,uz/j1f")
#     .option("driver","com.mysql.cj.jdbc.Driver")
#     .option("query", "select * from entries;")
#     .load()
# )

df1 = (
    spark
    .read
    .format("jdbc")
    .option("url","jdbc:mysql://10.48.0.3/guestbook?user=alazzaro&password={_F/y_73Q,uz/j1f")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .option("query", "select * from entries")
    .load()
)



df1.show()