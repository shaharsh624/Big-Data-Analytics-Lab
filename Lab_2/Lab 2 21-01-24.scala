// Databricks notebook source
// val -> mutable
// var -> immutable

// COMMAND ----------

var name = List(1, 2, 3, 4, 1, 4, 7, 5, 8, 5, 7)

// COMMAND ----------

name.distinct

// COMMAND ----------

name(2)

// COMMAND ----------

name.size

// COMMAND ----------

name.reverse

// COMMAND ----------

name.max

// COMMAND ----------

name.isEmpty

// COMMAND ----------

var num = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)

// COMMAND ----------

var lang = Array("Scala", "Python", "R")

// COMMAND ----------

lang.head

// COMMAND ----------

lang.tail

// COMMAND ----------

num(3)

// COMMAND ----------

num(3) = 13
num

// COMMAND ----------

import scala.collection.mutable.ArrayBuffer

// COMMAND ----------

var car = new ArrayBuffer[String]()

// COMMAND ----------

car.append("car1")
car.append("car2")
car.append("car3")

// COMMAND ----------

car

// COMMAND ----------

car += "car4"
car

// COMMAND ----------

car.length

// COMMAND ----------

// Remove element from end
car.trimEnd(1)

// COMMAND ----------

// Remove element from start
car.trimStart(1)

// COMMAND ----------

car

// COMMAND ----------

car.insert(2, "BMW")
car

// COMMAND ----------

car.insert(2, "Tata")
car

// COMMAND ----------

// Using num Array
num

// COMMAND ----------

// Square
num.map(x=>x*x)

// COMMAND ----------

// ERROR
car.map(y=>((y.toInt)+3).toString)

// COMMAND ----------

val a = num.map(aa => aa*(aa-1))

// COMMAND ----------

val b = num.map(a => a+1).map(b => b*b)

// COMMAND ----------

val fruits = List("Orange", "Banana", "Apple", "Pomogranate")

// COMMAND ----------

fruits.map(x=>(x, x.length))

// COMMAND ----------

fruits.filter(x => (x.length > 5))

// COMMAND ----------

var ratings = List(2.4, 5.6, 8.9, 6.7)
val marks = ratings.map(m => m*10)
val gradeB = marks.filter(m => (m>=60) & (m<80))

// COMMAND ----------

def add(a:Double = 100, b:Double = 200):Double = {
  var sum:Double = 0
  sum = a+b
  return sum
}

// COMMAND ----------

add()

// COMMAND ----------

var x = 1
var b = if (x<3) 
  println("Less than 3")
 else 
  println("Greater than 3")


// COMMAND ----------

var marks = 75
if (marks > 70) {
  print("A")
} else if (marks > 50 && marks < 70) {
  print("B")
} else {
  print("C")
}

// COMMAND ----------

def sq(x:Double):Double = {
  return x*x
}

// COMMAND ----------

def sqAdd(x:Double, y:Double):Double = {
  return sq(x) + sq(y)
}

// COMMAND ----------

sqAdd(3, 4)

// COMMAND ----------

for (i <- 1 to 10 ) {
  println(i)
  }

// COMMAND ----------

// Matrix Multiplication - not Working

var a = Array(List(12, 7, 3), List(4, 5, 6), List(7, 8, 9))
var b = Array(List(5, 8, 1), List(6, 7, 3), List(4, 5, 9))
var c = Array(List(0, 0, 0), List(0, 0, 0), List(0, 0, 0))

for (i <- 0 until a.length ) {
    for (j <- 0 until b.length ) {
      var sum = 0
          sum = c(i)(j) + a(i)(k) * b(k)(j)
            c(i)(j) = sum
        }
}

// COMMAND ----------


