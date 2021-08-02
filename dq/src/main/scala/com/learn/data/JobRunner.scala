package com.learn.data

import com.learn.data.expression.{Expression, ScalaExpressionHelper}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

object JobRunner extends Serializable {

  def run(spark: SparkSession, job: Job): Unit = {

    val ds = job.source.read(spark)

    def validateRow(row: Row): Row = {
      val ruleMap = job.validation.rules.map { rule =>
        val expr = Expression(rule.expr).resolveExpr(row.schema)
        val functionExpr = s"def function(row: org.apache.spark.sql.Row): Boolean = { $expr }"
        (rule, ScalaExpressionHelper.eval[Row, Boolean](functionExpr))
      }
      try {
        val status = ruleMap.find { case (rule, f) => rule.`type` == "error" && !f(row) }.map(_ => "error")
          .getOrElse {
            ruleMap.find { case (rule, f) => rule.`type` == "warn" && !f(row) }.map(_ => "warn")
              .getOrElse("success")
          }
        Row.fromSeq(row.toSeq :+ status)
      } catch {
        case _: Throwable => Row.fromSeq(row.toSeq :+ "corrupt")
      }
    }

    val rowValidator: Row => Row = validateRow

    val rowEncoder = RowEncoder(ds.schema.add("dq_status", StringType))

    val res = ds.mapPartitions(rowItr => rowItr.map(rowValidator))(rowEncoder)

    job.sink.write(res)

  }

}
