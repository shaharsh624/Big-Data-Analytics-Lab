// Databricks notebook source
val coins = sc.textFile("dbfs:/FileStore/shared_uploads/harsh.sce21@sot.pdpu.ac.in/coints-1.txt")

// COMMAND ----------

coins.collect

// COMMAND ----------

val splitCoins = coins.flatMap(line=>line.split(" "))

// COMMAND ----------

splitCoins.collect

// COMMAND ----------

val mapCoins = splitCoins.map(coin => (coin.toInt, 1))

// COMMAND ----------

mapCoins.collect

// COMMAND ----------

val reduceCoins = mapCoins.reduceByKey(_+_)

// COMMAND ----------

reduceCoins.collect

// COMMAND ----------

splitCoins.count

// COMMAND ----------

val df = spark.createDataFrame(reduceCoins).toDF("Coin","Count")
  df.foreach(f=> println(f))

val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")
df.foreach(f => {
  longAcc.add(f.getInt(1))
})
println("Total Number of Coins", longAcc.value)

// COMMAND ----------

val listRdd = spark.sparkContext.parallelize(List(reduceCoins))

// COMMAND ----------

println("Total : "+listRdd.fold(0)((acc,ele) => {acc + ele}))
println("Min : "+listRdd.fold(0)((acc,ele) => {acc min ele}))
println("Max : "+listRdd.fold(0)((acc,ele) => {acc max ele}))

// COMMAND ----------


