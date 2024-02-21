// Databricks notebook source
// MAGIC %md
// MAGIC Spark SQL

// COMMAND ----------

// %python

// from pyspark import SparkContext, SparkConf
// conf = 
// sc = SparkContext(conf = conf)

// COMMAND ----------

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// COMMAND ----------

val a = sc.parallelize(1 to 10)
a.collect

// COMMAND ----------

val b = a.map(x=>(x, x+1))
b.collect

// COMMAND ----------

// MAGIC %md
// MAGIC Dataframe

// COMMAND ----------

val df = b.toDF("First", "Second")
df.show

// COMMAND ----------

val a = List(("Tom", 5), ("Jerry", 2), ("Donald", 7))
val df = a.toDF("Name", "Age")
df.show

// COMMAND ----------

val a = Seq(("Tom", 5), ("Jerry", 2), ("Donald", 7))
val df = a.toDF("Name", "Age")
df.show

// COMMAND ----------

df.registerTempTable("Cartoon")

// COMMAND ----------

df.createOrReplaceTempView("Cartoon")

// COMMAND ----------

sqlContext.sql("select * from Cartoon where Name = 'Tom'").show

// COMMAND ----------

sqlContext.sql("select * from Cartoon").show

// COMMAND ----------

sqlContext.sql("select count(*) from Cartoon").show

// COMMAND ----------

val df1 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/harsh.sce21@sot.pdpu.ac.in/file-2.json")

// COMMAND ----------

df1.show

// COMMAND ----------

df1.printSchema()

// COMMAND ----------

df1.select("name", "Age").show

// COMMAND ----------

df1.createOrReplaceTempView("employee")

// COMMAND ----------

sqlContext.sql("select name from employee").show

// COMMAND ----------

df1.filter("age > '23'").show

// Alternate Way
df1.filter(df1("age") > 23).show

// COMMAND ----------

df1.groupBy("age").count().show

// COMMAND ----------

val rddb = sc.parallelize(List("BMW", "Mercedes", "Toyota", "Audi"))

// COMMAND ----------

rddb.partitions.length

// COMMAND ----------

val rdda = sc.parallelize(1 to 1000, 10)

// COMMAND ----------

rdda.partitions.length

// COMMAND ----------

rdda.count

// COMMAND ----------

rdda.first

// COMMAND ----------

rdda.take

// COMMAND ----------

rdda.saveAsTextFile("dbfs:/FileStore/shared_uploads/harsh.sce21@sot.pdpu.ac.in/parralel-file.txt")

// COMMAND ----------

val rddaRead = sc.textFile("dbfs:/FileStore/shared_uploads/harsh.sce21@sot.pdpu.ac.in/parralel-file.txt")

// COMMAND ----------

rddaRead.count()

// COMMAND ----------

val book = sc.textFile("dbfs:/FileStore/shared_uploads/harsh.sce21@sot.pdpu.ac.in/sample_text.txt")

// COMMAND ----------

book.collect

// COMMAND ----------

val a = book.flatMap(x=>x.split(" ")).collect()

// COMMAND ----------

val b = a.map( y=> (y, 1))

// COMMAND ----------

val a = book.flatMap(x=>x.split(" ")).map( y=> (y, 1)).reduceByKey((x, y) => (x+y)).collect

// COMMAND ----------

// Descending oreder - false
val a = book.flatMap(x=>x.split(" ")).map( y=> (y, 1)).reduceByKey((x, y) => (x+y)).sortBy(_._1, false).collect

// COMMAND ----------

// Ascending oreder - true
val a = book.flatMap(x=>x.split(" ")).map( y=> (y, 1)).reduceByKey((x, y) => (x+y)).sortBy(_._2, true).collect

// COMMAND ----------

val a = book.flatMap(x=>x.split(" ")).map( y=> (y, 1)).reduceByKey((x, y) => (x+y)).sortBy(_._2, true).take(10)

// COMMAND ----------

val a = book.flatMap(x=>x.split(" ")).map( y=> (y, 1)).reduceByKey((x, y) => (x+y)).sortBy(_._2, true).filter(_._2 > 1).collect

// COMMAND ----------


