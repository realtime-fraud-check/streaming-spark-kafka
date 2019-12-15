package utils

import java.io.File
import java.nio.file.Path

import com.typesafe.config.{Config, ConfigFactory}

object ApplicationConfigurator {

  def getConfig(file:File):Config  = {
    //Code to  read the config file and return back application config object
    ConfigFactory.parseFile(file)
  }
}

case class ApplicationConfig()
