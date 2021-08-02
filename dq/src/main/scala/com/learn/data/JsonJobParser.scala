package com.learn.data

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.collection.JavaConverters._

object JsonJobParser {

  def parse(json: String): Job = {
    try {
      val jsonObject = new ObjectMapper().readTree(json)
      parse(jsonObject)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        throw new RuntimeException("Unable to parse job json", ex)
    }
  }

  def parse(node: JsonNode): Job = {

    val sourceNode = node.get("source")
    val dataSource = sourceNode.get("type").asText().trim() match {
      case "csv" => parseCSVDataSource(sourceNode)
      case other => throw new IllegalArgumentException(s"Unsupported data source format $other")
    }

    val validationNode = node.get("validation")
    val rules = validationNode.get("rules").asScala.map { ruleNode =>
      Rule(
        ruleNode.get("id").asText(),
        ruleNode.get("name").asText(),
        ruleNode.get("type").asText(),
        ruleNode.get("expr").asText()
      )
    }

    val sinkNode = node.get("sink")
    val sink = sinkNode.get("type").asText().toLowerCase match {
      case "debug" => DebugSink(Option(sinkNode.get("sample")).map(_.asInt()).getOrElse(10))
      case "csv" => parseCSVDataSink(sinkNode)
      case _ => null
    }

    Job(dataSource, Validation(rules.toList), sink)

  }

  private def parseCSVDataSource(node: JsonNode): CSVDataSource = {
    CSVDataSource(
      node.get("fileLocation").asText(),
      node.get("delimiter").asText(),
      node.get("firstRowHeader").asBoolean(false)
    )
  }

  private def parseCSVDataSink(node: JsonNode): CSVSink = {
    CSVSink(
      node.get("fileLocation").asText(),
      node.get("delimiter").asText(),
      node.get("header").asBoolean(false),
      node.get("mode").asText(),
      Nil, None
    )
  }

}
