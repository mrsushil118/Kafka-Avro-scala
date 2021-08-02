package com.learn.data.utils

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUtil {

  def parseFromFile(filepath: String): Config = {
    val file = new File(filepath.trim)
    if (file.exists()) {
      ConfigFactory.parseFile(file)
    } else {
      throw new IllegalArgumentException(s"Config file doesn't exist: $filepath")
    }
  }

  def parseFromResource(resource: String): Config = {
    ConfigFactory.parseResources(resource)
  }

}
