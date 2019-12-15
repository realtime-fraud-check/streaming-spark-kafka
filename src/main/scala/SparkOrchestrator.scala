import java.io.File
import java.nio.file.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import streamprocessing.StreamProcess
import utils.ApplicationConfigurator

object SparkOrchestrator {


  def main(args: Array[String]): Unit = {
    //Based on the args we will start stream or batch processing.
    if (args.length != 2) {
      println(s"Invalid number of arguments passsed. Please pass the StreamingProcess flag & Hive Support flag")
      System.exit(-1)
    }

    //Hard coded the spark app name.
    //Can be passed using either config file or as an argument.

    val streamProcessingFlag = args(0).toBoolean
    val hiveSupportEnabled = args(1).toBoolean
    val batchAppName = "FraudAnalysisApp-Batch"
    val streamingAppName = "FraudAnalysisApp-Streaming"



    //Creating Spark Session
    def sparkSession = if (streamProcessingFlag) {
      createSparkSession(streamingAppName, hiveSupportEnabled)
    } else {
      createSparkSession(batchAppName, hiveSupportEnabled)
    }

    process(sparkSession, streamProcessingFlag)

  }

  def process(sparkSession: SparkSession, isStreamProcess: Boolean = true) = {
    if (isStreamProcess) {
      StreamProcess.process(sparkSession)
    } else {
      //Do batch processing}
    }
  }

  def createSparkSession(appName: String,
                         hiveEnabled: Boolean = true): SparkSession = {
    //val ApplicationName = ApplicationConfigurator.getConfig(new File("SomePath"))
    if (hiveEnabled) {
      return SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()
    }
    return SparkSession.builder().appName(appName).getOrCreate()
  }

}
