// Databricks notebook source
import scala.io.Source
val html = Source.fromURL("https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json")
dbutils.fs.rm("/tmp/baby.json",recurse=true)
dbutils.fs.put("/tmp/baby.json",html.mkString)

// COMMAND ----------

val jsonDs = sc.wholeTextFiles("/tmp/baby.json")
                  .map(_._2.replace("\n", "").replace(" ", "")).toDS()
val babyDs = spark.read.json(jsonDs)
babyDs.createOrReplaceTempView("babyfulldata")
import org.apache.spark.sql.functions.explode

val explodedDataDs = babyDs.select(explode($"data").as("data"))
val dataDF = explodedDataDs.as[Array[String]].map { case data => {
      (data(0).toInt, data(1), data(2).toInt, data(3).toInt, data(4), data(5).toInt, data(6), data(7), data(8), data(9), data(10),
        data(11), data(12))
      }
    }.toDF("sid", "id", "position", "created_at", "created_meta", "updated_at", "updated_meta", "meta", "Year", "firstName", "County", "Sex", "Count")


// COMMAND ----------

dataDF.rdd.partitions.length

// df.rdd.partitions.length

// COMMAND ----------

spark.catalog.setCurrentDatabase("ashish")

// spark.catalog.listDatabases.show

// spark.catalog.getTable("stuff")

// COMMAND ----------

// MAGIC %sql show partitions scans_flatten_2

// COMMAND ----------

val df = spark.sql("select * from scans_flatten_2")
df.rdd.getNumPartitions

// COMMAND ----------

// MAGIC %sql select count(*) from scans_flatten_2

// COMMAND ----------

val salesSeq = Seq(
      (111,1,1,100.0),
      (112,2,2,505.0),
      (115,1,2,500.0),
      (121,1,1,100.0))

//Example Rdd
val salesRdd = sc.parallelize(salesSeq)

//Example DataFrame
val salesDataFrame = salesRdd.toDF

//Example Dataset
case class sales(transactionId:Int,customerId:Int,itemId:Int,amountPaid:Double)
val salesDataset = salesRdd.map(row => sales(row._1, row._2, row._3, row._4)).toDF.as[sales]

// COMMAND ----------

df.queryExecution.debug.codegen()