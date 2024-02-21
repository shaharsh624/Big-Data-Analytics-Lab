// Databricks notebook source
// Resilient Distributed Dataset (RDD)
val a = sc.parallelize(List("A", "B", "C", "D"))

// COMMAND ----------

// Method 1
val b = a.map(x=> (x, 1))
b.collect

// COMMAND ----------

// Method 2
val b = a.map((_, 1))
b.collect

// COMMAND ----------

val b = a.map(x=> (x, x.length))
b.collect

// COMMAND ----------

val a = sc.parallelize(List(1, 2, 3, 4, 5)).map(x=>(List(x, x, x))).collect

// COMMAND ----------

rdda.filter(_.contains("b")).collect

// COMMAND ----------

val rdda = sc.parallelize(List("aaaa", "bbbb", "cccc"))
rdda.filter(_.equals("aaaa")).collect

// COMMAND ----------



// COMMAND ----------


