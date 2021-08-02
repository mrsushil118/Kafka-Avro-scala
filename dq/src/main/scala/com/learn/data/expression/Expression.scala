package com.learn.data.expression

import java.util.regex.Pattern

import org.apache.spark.sql.types._

import scala.util.matching.Regex

case class Expression(expr: String) {

  require(expr.trim.nonEmpty, s"Expression should not be empty: $expr")

  // List of field references used in expression.
  def fieldRefList: List[String] = Expression.FIELD_REF_REGEX.findAllIn(expr).matchData
    .flatMap(_.subgroups).toList.distinct

  /**
    * Helper method that resolves every field reference used in expression.
    *
    * Example:
    * Input expression => ${buyDetails}.nonEmpty
    * Output expression => row.getAs[String]("orderID").nonEmpty
    */
  def resolveExpr(schema: StructType): String = {
    fieldRefList.fold(expr) { case (resolvedExpr, fieldRef) =>
      val field = schema.apply(fieldRef)
      resolvedExpr.replace(
        "${" + fieldRef + "}".r,
        Expression.getExprForGetValueFromRow(field.dataType, fieldRef)
      )
    }
  }

}

object Expression {

  // Pattern to find the field references in the expression
  // Ex: ${orderID}
  val FIELD_REF_REGEX: Regex = "\\$\\{(.*?)\\}".r

  // private val primitiveTypes = Seq("Boolean", "Int", "Long", "Double", "Float", "Short", "Byte")
  private val ID_PATTERN = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*"

  // Check whether a given string is a fully qualified class name.
  def isValidClassName(str: String): Boolean = {
    Pattern.compile(ID_PATTERN + "(\\." + ID_PATTERN + ")*").matcher(str).matches()
  }


  def extractValuesFromExpression(expression: String): (String, List[String]) = {
    val expressionNameIndexEnd = expression.indexOf("(")
    val expressionName = expression.substring(1, expressionNameIndexEnd)
    val parametersRegex: Regex = """(?<=\()[^()]*""".r
    val parameters = parametersRegex.findAllIn(expression).toList.flatMap(_.split(","))

    (expressionName, parameters)
  }

  /** Returns the specialized code to access fieldRef value from BWRow. */
  private def getExprForGetValueFromRow(dataType: DataType, field: String): String = {
    dataType match {
      case _: StructType => "row.getAs[BWRow](\"" + field + "\")"
      // TODO: Need to find a way to get field value(in case of null) from row as primitive data type.
      // case dt: String if primitiveTypes.contains(toScalaDataType(dt)) => s"get$dt" + "(\"" + field + "\")"
      case _: DecimalType => "Option(row.getAs[java.math.BigDecimal](\"" + field + "\")).map(scala.math.BigDecimal(_)).orNull"
      case dt => "row.getAs[" + toScalaDataType(dt) + "](\"" + field + "\")"
    }
  }

  // Standardise the given data type string to Scala type.
  private def toScalaDataType(dataType: DataType): String = {
    dataType match {
      case IntegerType => "Int"
      case StringType => "String"
      case BooleanType => "Boolean"
      case _ => throw new RuntimeException(s"$dataType is not a valid data type in scala")
    }
  }

}
