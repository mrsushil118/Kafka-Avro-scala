package com.learn.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object TestApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Test")
      .master("local[*]")
      .getOrCreate()


    val schema = StructType(
      List(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("_corrupt_record", StringType, nullable = true)
      )
    )

    val df = spark.read
      .option("header", "true")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .option("mode", "PERMISSIVE")
      //.schema(schema)
      .csv("C:\\Users\\suskumar9\\Documents\\projects\\data-quality\\src\\main\\resources\\person.csv")


    df.printSchema()
    df.show()

  }

}

