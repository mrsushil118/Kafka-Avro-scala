package com.learn.data

import org.apache.spark.sql.{Dataset, Row, SparkSession}

trait DataSource {
  def read(spark: SparkSession): Dataset[Row]
}

case class CSVDataSource(
                          fileLocation: String,
                          delimiter: String,
                          header: Boolean = false
                        ) extends DataSource {

  override def read(spark: SparkSession): Dataset[Row] = {
    val csvDFReader = spark.read.format("csv")
      .option("delimiter", delimiter)
      .option("header", header)
    csvDFReader.option("inferSchema", "true").load(fileLocation)
  }

}