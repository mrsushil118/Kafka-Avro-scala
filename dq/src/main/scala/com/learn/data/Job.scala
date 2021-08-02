package com.learn.data

case class Job(
                source: DataSource,
                validation: Validation,
                sink: DataSink
              )

case class Validation(rules: Seq[Rule])

case class Rule(id: String, name: String, `type`: String, expr: String)

case class Optimize(numPartition: Option[Int], keys: Option[Seq[String]])

