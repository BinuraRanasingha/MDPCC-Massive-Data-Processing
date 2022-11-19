// Databricks notebook source
//Spark RDD
//Creating an RDD
//Method 1 - Parallelized Connections
val data = Array(1,2,3,4,5)
val distData = sc.parallelize(data)

// COMMAND ----------

//Method 2 - External Datasets
val data = sc.textFile("/FileStore/tables/txtfile.txt")
data.collect().foreach(println)

// COMMAND ----------

//Method 3 - Create from another RDD
val rdd = sc.parallelize(Seq(("Java",20000),("Python",100000),("Scala",3000)))
val rdd_new = rdd.map(row=>{(row._1,row._2 + 100)})
rdd_new.collect()

// COMMAND ----------

//Writing a code in spark to get the word count of the "textfile.txt"
val splitdata = data.flatMap(line=>line.split(" "))
splitdata.collect
val mapdata = splitdata.map(word=>(word,1))
mapdata.collect
val reducedata = mapdata.reduceByKey(_+_)
reducedata.collect.foreach(println)

// COMMAND ----------

//Sparks SQL
//Spark DataFrames and Datasets
//Creating DataFrames Method 1 - Creating from todDF()
import spark.implicits._
val data = Seq((8,"bat"),(14,"cat"),(22,"tiger"),(24,"lion"))
val df1 = data.toDF("number","word")
df1.show

// COMMAND ----------

//Method 2 - Creating from createDataFrame()
import org.apache.spark.sql.types._
val data1 = Seq(Row(8,"bat"),Row(64,"mouse"),Row(-27,"horse"))
val Schema = List(StructField("number",IntegerType,true),StructField("word",StringType,true))
val df2 = spark.createDataFrame(sc.parallelize(data1),StructType(Schema))
df2.show

// COMMAND ----------

//Method 3 - Creating DataFrames from Data sources
val df3 = spark.read.json("/FileStore/tables/people.json")
df3.show

// COMMAND ----------

//Printing the schema in a tree format
df3.printSchema()

// COMMAND ----------

//Select only the name column
df3.select("name").show()

// COMMAND ----------

//Select everybody, but increment the age by 1
df3.select($"name",$"age" + 1).show()

// COMMAND ----------

// Select people older than 21
df3.filter($"age" > 21).show()
df3.where($"age" > 15).show()

// COMMAND ----------

// Count people by age
df3.groupBy("age").count().show()

// COMMAND ----------

//Replace null values using DataFrame Na function
val nonNullDF = df3.na.fill(999)
display(nonNullDF)

// COMMAND ----------

// Retrieve rows with missing age
val filterNonNullDF = nonNullDF.filter($"age" === 999 || $"name" === "--").sort($"name".asc)
display(filterNonNullDF)

// COMMAND ----------

//change column type
import org.apache.spark.sql.functions.col
val dfnewtype = df3.withColumn("age",col("age").cast(IntegerType))

// COMMAND ----------

//add a column
import org.apache.spark.sql.functions.lit
val df4 = df3.withColumn("Country",lit("USA"))
df4.show

// COMMAND ----------

//Union two DataFrames
val unionDF = df1.union(df2)
display(unionDF)

// COMMAND ----------

//Running SQL Queries Programmatically
// Register the DataFrame as a SQL temporary view
df3.createOrReplaceTempView("people")
val sqlDF = spark.sql("select * from people")
sqlDF.show()

// COMMAND ----------

//Creating Datasets (only in Scala and Java)
// Encoders are created for case classes
case class Person(name: String, age: Long)
val caseClassDS = Seq(Person("Andy",32)).toDS()
caseClassDS.show()

// COMMAND ----------

// DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
val path = "/FileStore/tables/people.json"
val peopleDS = spark.read.json(path).as[Person]
peopleDS.show()

// COMMAND ----------



// COMMAND ----------


