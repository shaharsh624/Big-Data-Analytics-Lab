// Databricks notebook source
val a = sc.parallelize(List(("Mumbai", 4000), ("Delhi", 2000), ("Chennai", 2000), ("Kolkata", 7000)))

// COMMAND ----------

a.filter(_._2.equals(4000)).collect

// COMMAND ----------

a.filter(_._2.equals(2000)).collect

// COMMAND ----------

a.filter(_._2 > 2000).collect

// COMMAND ----------

// Random Number - without repitition
val a = sc.parallelize(1 to 1000)
a.sample(false, 0.1, 100).collect

// COMMAND ----------

// Random Number - With repitition
val a = sc.parallelize(1 to 1000)
a.sample(true, 0.2, 100).collect

// COMMAND ----------

val b = sc.parallelize(List(1,2,2,1,1,2))
b.sample(true, 0.5, 15).collect

// COMMAND ----------

val a = sc.parallelize(1 to 7)
val b = sc.parallelize(5 to 10)

// COMMAND ----------

a.union(b).collect

// COMMAND ----------

a.intersection(b).collect

// COMMAND ----------

a.union(b).distinct.collect

// COMMAND ----------

val a = sc.parallelize(1 to 9, 3)

// COMMAND ----------

a.mapPartitions(x=>List(x.next).iterator).collect

// COMMAND ----------

val a = sc.parallelize(1 to 9, 4)

// COMMAND ----------

a.mapPartitions(x=>List(x.next).iterator).collect

// COMMAND ----------

def pra(index:Int, iter:Iterator[(Int)]) : Iterator[String] = 
{
  iter.toList.map(x => x + " " + index).iterator
}

// COMMAND ----------

val a = sc.parallelize(List(1,2,3,4,5,6), 3)

// COMMAND ----------

a.mapPartitionsWithIndex(pra).collect

// COMMAND ----------

def isPrime(num:Int): Boolean = 
(num>1) && !(2 to scala.math.sqrt(num).toInt).exists(x => num % x == 0)

// COMMAND ----------

val a = sc.parallelize(1 to 10)
val even = a.filter(x => (x%2==0)).map(x => x*x).sum

// COMMAND ----------

val odd = a.filter(x => (x%2!=0)).map(x => x*x).sum

// COMMAND ----------

val prime = a.filter(isPrime).map(x => x*x).sum

// COMMAND ----------

println("Odd: " + odd)
println("Even: " + even)
println("Prime: " + prime)

// COMMAND ----------

val b = a.foreach(isPrime)

// COMMAND ----------

a.filter(isPrime).collect

// COMMAND ----------

// Read a file with multiple lines inside and perform the following to get the word count

val data = sc.textFile("dbfs:/FileStore/shared_uploads/harsh.sce21@sot.pdpu.ac.in/file.txt")
data.collect
val splitdata = data.flatMap(line => line.split(" "))
val mapdata = splitdata.map(word => (word, 1))
mapdata.collect
val reducedata = mapdata.reduceByKey(_+_)
reducedata.collect

// COMMAND ----------

reducedata.count

// COMMAND ----------

// Read a file with multiple lines inside and perform the following to get the character count

val data = sc.textFile("dbfs:/FileStore/shared_uploads/harsh.sce21@sot.pdpu.ac.in/file.txt")
data.collect
val splitdata = data.flatMap(line => line.split(""))
val mapdata = splitdata.map(character => (character, 1))
mapdata.collect
val reducedata = mapdata.reduceByKey(_+_)
reducedata.collect

// COMMAND ----------

// Default Partition - 8

val rdda = sc.parallelize(List(1,2,3,4,5))
val rddb = rdda.collect
println("Partitions: " + rdda.getNumPartitions)
println("First" + rdda.first)
rdda.foreach(println)

// COMMAND ----------

val rdda = sc.parallelize(List(1,2,3,4,5), 100)
val rddb = rdda.collect
println("Partitions: " + rdda.getNumPartitions)
println("First" + rdda.first)
rdda.foreach(println)

// COMMAND ----------

// Seq is mutable where as List is not mutable
val rddc = sc.parallelize(Seq.empty[String])
rddc.collect

// COMMAND ----------

val b = rddb.map((_, 2))

// COMMAND ----------

val broadcastVar = sc.broadcast(Array(1, 2, 3))
broadcastVar.value

// COMMAND ----------

import org.apache.spark.sql.SparkSession

val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
val countries = Map(("USA","United States of America"),("IN","India"))
val broadcastStates = spark.sparkContext.broadcast(states)
val broadcastCountries = spark.sparkContext.broadcast(countries)
val data = Seq(("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  )
val rdd = spark.sparkContext.parallelize(data)
//rdd.collect
val rdd2 = rdd.map(f=>{
  val country = f._3
  val state = f._4
  val fullCountry = broadcastCountries.value.get(country).get
  val fullState = broadcastStates.value.get(state).get
  (f._1,f._2,fullCountry,fullState)
})

println(rdd2.collect().mkString("\n"))

// COMMAND ----------

// import org.apache.spark.sql.SparkSession

// val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
// val countries = Map(("USA","United States of America"),("IN","India"))
// val broadcastStates = spark.sparkContext.broadcast(states)
// val broadcastCountries = spark.sparkContext.broadcast(countries)
// val data = Seq(("James","Smith","USA","CA"),
//     ("Michael","Rose","USA","NY"),
//     ("Robert","Williams","USA","CA"),
//     ("Maria","Jones","USA","FL")
//   )

// val columns = Seq("firstname", "lastname", "country", "")
// val rdd = spark.sparkContext.parallelize(data)
// //rdd.collect
// val rdd2 = rdd.map(f=>{
//   val country = f._3
//   val state = f._4
//   val fullCountry = broadcastCountries.value.get(country).get
//   val fullState = broadcastStates.value.get(state).get
//   (f._1,f._2,fullCountry,fullState)
// })

// println(rdd2.collect().mkString("\n"))

// COMMAND ----------

val accum = sc.longAccumulator("SumAccumulator")

// COMMAND ----------

sc.parallelize(Array(1, 2, 3)).foreach(x => accum.add(x))
accum.value

// COMMAND ----------

val inputRDD = spark.sparkContext.parallelize(List(("Z",1),("A",20),("B",30),("C",40),("B",30),("B",60)))

val listRDD = spark.sparkContext.parallelize(List(1,2,3,4,5,6,7))

// COMMAND ----------

def param0 = (accu:Int, v:Int) => accu + v
def param1 = (accu1:Int, accu2:Int) => accu1 + accu2
println("Aggregate: "+listRDD.aggregate(0)(param0,param1))

def param3 = (accu:Int, v:(String,Int)) => accu + v._2
def param4 = (accu1:Int, accu2:Int) => accu1 + accu2
println("Aggregate: "+inputRDD.aggregate(0)(param3,param4))

// COMMAND ----------


