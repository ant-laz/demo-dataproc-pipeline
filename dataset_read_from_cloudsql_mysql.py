# options for spark.read.format(jdbc) from here:
# https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
# The name of the class that implements java.sql.Driver 
# in MySQL Connector/J is com.mysql.cj.jdbc.Driver.
# https://dev.mysql.com/doc/connector-j/8.1/en/connector-j-reference-driver-name.html

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("myapp").getOrCreate()

#######################################################################################
# Method 1
# SUCCESS 
# specify IP address assigned to MySQL
# username & password on URL
#######################################################################################
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
########################################################################################
# Method 2
# SUCCESS 
# local host 3307 is where cloudsql proxy is listening
# username & password on URL
########################################################################################
df2 = (
    spark
    .read
    .format("jdbc")
    .option("url","jdbc:mysql://localhost:3307/guestbook?user=alazzaro&password={_F/y_73Q,uz/j1f")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .option("query", "select * from entries")
    .load()
)
df2.show()
########################################################################################
# Method 7
# SUCCESS
# local host 3307 is where cloudsql proxy is listening
# username & password passed to the spark read command as options
########################################################################################
df7 = (
    spark
    .read
    .format("jdbc")
    .option("url","jdbc:mysql://localhost:3307/guestbook")
    .option("driver","com.mysql.cj.jdbc.Driver")
    .option("user","alazzaro")
    .option("password", "{_F/y_73Q,uz/j1f")
    .option("query", "select * from entries")
    .load()
)
df7.show()
########################################################################################
# Method 3
# FAIL 
# local host 3307 is where cloudsql proxy is listening
# no username & pass on URL. Service account of cluster has auth for cloudsql
# ERROR "Access denied for user 'yarn'@'cloudsqlproxy~10.154.0.35' (using password: NO)"
########################################################################################
# df3 = (
#     spark
#     .read
#     .format("jdbc")
#     .option("url","jdbc:mysql://localhost:3307/guestbook")
#     .option("driver","com.mysql.cj.jdbc.Driver")
#     .option("query", "select * from entries")
#     .load()
# )
# df3.show()
########################################################################################
# Method 4
# FAIL
# local host 3307 is where cloudsql proxy is listening
# no username & pass on URL. Service account of cluster has auth for cloudsql
# provide cloudsql instance as url parameter
# ERROR "Access denied for user 'yarn'@'cloudsqlproxy~10.154.0.35' (using password: NO)"
########################################################################################
# df4 = (
#     spark
#     .read
#     .format("jdbc")
#     .option("url","jdbc:mysql://localhost:3307/guestbook?cloudSqlInstance=dataproc-demo-400309:europe-west2:cloudsql-instance")
#     .option("driver","com.mysql.cj.jdbc.Driver")
#     .option("query", "select * from entries")
#     .load()
# )
# df4.show()
########################################################################################
# Method 5
# FAIL
# local host 3307 is where cloudsql proxy is listening
# no username & pass on URL. Service account of cluster has auth for cloudsql
# provide cloudsql instance as url parameter
# provide socket factory as argument
# ERROR "Cannot connect to MySQL server on localhost:3,307."
########################################################################################
# df5 = (
#     spark
#     .read
#     .format("jdbc")
#     .option("url","jdbc:mysql://localhost:3307/guestbook?cloudSqlInstance=dataproc-demo-400309:europe-west2:cloudsql-instance&socketFactory=com.google.cloud.sql.mysql.SocketFactory")
#     .option("driver","com.mysql.cj.jdbc.Driver")
#     .option("query", "select * from entries")
#     .load()
# )
# df5.show()
########################################################################################
# Method 6
# FAIL
# local host 3307 is where cloudsql proxy is listening
# no username & pass on URL. Service account of cluster has auth for cloudsql
# provide cloudsql instance as url parameter
# provide cluster service account as user
# ERROR Access denied for user '72693102885-compute@developer.gserviceaccount.co'@'cloudsqlproxy~10.154.0.33' (using password: NO)
########################################################################################
# df6 = (
#     spark
#     .read
#     .format("jdbc")
#     .option("url","jdbc:mysql://localhost:3307/guestbook?cloudSqlInstance=dataproc-demo-400309:europe-west2:cloudsql-instance&user=72693102885-compute@developer.gserviceaccount.com")
#     .option("driver","com.mysql.cj.jdbc.Driver")
#     .option("query", "select * from entries")
#     .load()
# )
# df6.show()




