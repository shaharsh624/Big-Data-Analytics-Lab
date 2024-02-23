// Databricks notebook source
import org.apache.spark.sql.SparkSession

val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"),("Beans",1500,"China"))

val df = spark.createDataFrame(data).toDF("Product","Amount","Country")
  df.foreach(f=> println(f))

// COMMAND ----------

val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")
df.foreach(f => {
  longAcc.add(f.getInt(1))
})
println("Accumulator Value", longAcc.value)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Extra to implement
// MAGIC
// MAGIC https://sparkbyexamples.com/spark/spark-read-text-file-rdd-dataframe/

// COMMAND ----------

 // fold example
  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))
  println("Partitions : "+listRdd.getNumPartitions)
  println("Total : "+listRdd.fold(0)((acc,ele) => {acc + ele}))
  println("Total with init value 2 : "+listRdd.fold(2)((acc,ele) => {acc + ele}))
  println("Min : "+listRdd.fold(0)((acc,ele) => {acc min ele}))
  println("Max : "+listRdd.fold(0)((acc,ele) => {acc max ele})) 

// COMMAND ----------

 val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),
("C", 40),("B", 30),("B", 60)))

  println("Total : "+inputRDD.fold(("",0))( (acc,ele)=>{ ("Total", acc._2 + ele._2)  }))
  println("Min : "+inputRDD.fold(("",0))( (acc,ele)=>{ ("Min", acc._2 min ele._2)  }))
  println("Max : "+inputRDD.fold(("",0))( (acc,ele)=>{ ("Max", acc._2 max ele._2)  }))

// COMMAND ----------


