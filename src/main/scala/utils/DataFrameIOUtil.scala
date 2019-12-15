package utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

//TODO need to figure out whether to keep this a as standalone object or a class
object DataFrameIOUtil {

  //As of now considering only the Subscribe mode of reading
  //TODO add code for assign mode of reading dataframe
  //TODO make spark session implict.
  def readKafkaAsDataFrame(sparkSession: SparkSession, topic: String, streamingFlag: Boolean): DataFrame ={
    if(streamingFlag){
      return readKafkaAsStreamingDataFrame(sparkSession)
    } else {return readKafkaAsStaticDataFrame(sparkSession)}
  }
//TODO make kafka options config driver rather than hardcoded

  def readKafkaAsStaticDataFrame(sparkSession: SparkSession): DataFrame = {
        val kafkaDataFrame = sparkSession.read
          .format("kafka")
          .option("kafka.bootstrap.servers","localhost:9092")
          .option("subscribe","topic")
          .option("startingOffsets", "earliest")
          .option("endingOffsets", "latest")
          .option("failOnDataLoss",true)
          .option("maxOffsetsPerTrigger", 10)
          .load()

    return kafkaDataFrame
  }

  def readKafkaAsStreamingDataFrame(sparkSession: SparkSession): DataFrame = {

    val kafkaStreamingDataFrame = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","topic")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("failOnDataLoss",true)
      .option("maxOffsetsPerTrigger", 10)
      .load()

    return kafkaStreamingDataFrame

  }

  def readCassandraAsDataFrame(sparkSession: SparkSession, table: String, keySpace: String): DataFrame = {

    val cassandraDataFrame = sparkSession.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpace, "table" -> table))
      .load()

    /**
      *  Creates a C* table based on the DataFrame Struct provided. Optionally
      *  takes in a list of partition columns or clustering columns names. When absent
      *  the first column will be used as the partition key and there will be no clustering
      *  keys.
      *
      *  Example:    cassandraDataFrame.createCassandraTable(keyspace,table,partitionKey,clusteringKey)
      */

    return cassandraDataFrame

  }

  def writeDataFrameToCassandra(sparkSession: SparkSession):Unit = {

  }

  def writeDataFrameToHiveTable(sparkSession: SparkSession):Unit = {

  }

  def checkPointDataToCassandra(sparkSession: SparkSession):Unit = {

  }

}
