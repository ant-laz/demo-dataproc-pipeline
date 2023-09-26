#!/usr/bin/env python

# from tutorial here
# https://cloud.google.com/dataproc/docs/tutorials/gcs-connector-spark-tutorial

# This example uses the legacy RDD spark programming interface
# RDD = Resilient Distributed Dataset 
# The newer & more performance spark programming interface is called Dataset
# docs on RDD
# https://spark.apache.org/docs/latest/rdd-programming-guide.html
# docs on Dataset
# https://spark.apache.org/docs/latest/sql-programming-guide.html

import pyspark
import sys

# sys is a built-in python library
# sys.argv is a list of command line arguments passed into the python script
# https://docs.python.org/3/library/sys.html#sys.argv
if len(sys.argv) != 3:
  raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

# gs://${GCS_BUCKET_NO_PREFIX}/input/
inputUri=sys.argv[1]

# gs://${GCS_BUCKET_NO_PREFIX}/output/
outputUri=sys.argv[2]

# Main entry point for Spark functionality. 
# A SparkContext represents the connection to a Spark cluster
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.html
sc = pyspark.SparkContext()

# Read a text file from HDFS, a local file system (available on all nodes), 
# or any Hadoop-supported file system URI, and return it as an RDD of Strings.
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.SparkContext.textFile.html#pyspark.SparkContext.textFile
lines = sc.textFile(sys.argv[1])

# lines is a RDD 
# A Resilient Distributed Dataset (RDD), 
# the basic abstraction in Spark. 
# Represents an immutable, partitioned collection of elements 
# that can be operated on in parallel.
# Class RDD has method flatMap that returns a new RDD by first applying a function 
# to all elements of this RDD, and then flattening the results.
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.flatMap.html#pyspark.RDD.flatMap
words = lines.flatMap(lambda line: line.split())

# words is a RDD
# RDD class has method map that 
# Return a new RDD by applying a function to each element of this RDD.
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.map.html
# RDD class has method reduceByKey
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.reduceByKey.html
wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda count1, count2: count1 + count2)

# wordCounts is a RDD
# RDD class has method saveAsTextFile
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.saveAsTextFile.html
wordCounts.saveAsTextFile(sys.argv[2])