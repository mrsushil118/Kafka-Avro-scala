package com.learn.data

import org.apache.spark.sql.Dataset

trait DataSink {
  def write[T](ds: Dataset[T])
}

case class CSVSink(
                    fileLocation: String,
                    delimiter: String,
                    header: Boolean,
                    mode: String,
                    partitionBy: Seq[String],
                    codec: Option[String]
                  ) extends DataSink {

  override def write[Row](ds: Dataset[Row]): Unit = {
    ds.write.mode("csv")
      .option("delimiter", delimiter)
      .option("header", header.toString)
      .option("compression", codec.getOrElse("none"))
      .partitionBy(partitionBy: _*)
      .csv(fileLocation)
  }

}


case class DebugSink(sample: Int) extends DataSink {
  override def write[Row](ds: Dataset[Row]): Unit = {
    ds.show(sample, truncate = false)
  }
}
