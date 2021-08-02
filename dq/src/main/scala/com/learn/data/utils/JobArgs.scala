package com.learn.data.utils

import scopt.OptionParser

case class JobArgs(
                    configFile: String = "",
                    jobFile: String = "",
                    jobParams: Map[String, String] = Map(),
                    argv: List[String] = List.empty[String]
                  )

object JobArgs {

  val parser: OptionParser[JobArgs] = new scopt.OptionParser[JobArgs]("Data-Quality") {

    head("!!! Spark job arguments !!!")

    opt[String]("app-conf-file")
      //.required()
      .action((x, c) => c.copy(configFile = x))
      .text("App config file location. It is required.")

    opt[String]("job-file")
      .required()
      .action((x, c) => c.copy(jobFile = x))
      .text("Job file location. It is required.")

    opt[Map[String, String]]("params").valueName("k1=v1,k2=v2...").action { (x, c) =>
      c.copy(jobParams = x)
    } text "Parameters in key/value pair for the job."

    arg[String]("arg...").optional().unbounded() action { (a, c) =>
      c.copy(argv = c.argv :+ a)
    } text "optional unbounded list of arguments."

  }

  def parse(args: Seq[String], defaultArgs: JobArgs): Option[JobArgs] = parser.parse(args, JobArgs())

}
