package com.learn.data

import com.learn.data.utils.{ConfigUtil, JobArgs}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object DQApp {

  def main(args: Array[String]): Unit = {

    val jobArgs = JobArgs.parse(args.toList, JobArgs()).getOrElse {
      throw new IllegalArgumentException("Invalid arguments, aborting...")
    }

    val appConfig = if (jobArgs.configFile.trim.nonEmpty) {
      ConfigUtil.parseFromFile(jobArgs.configFile)
    } else ConfigUtil.parseFromResource("app.conf")

    val job = JsonJobParser.parse(scala.io.Source.fromFile(jobArgs.jobFile).mkString)

    val sparkConf = new SparkConf(loadDefaults = true).setMaster("local[*]")

    appConfig.getConfig("data.spark.conf").entrySet().asScala.foreach { c =>
      sparkConf.set(c.getKey, c.getValue.unwrapped().toString)
    }

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    try {
      JobRunner.run(sparkSession, job)
    } finally {
      sparkSession.stop()
    }
  }

}
