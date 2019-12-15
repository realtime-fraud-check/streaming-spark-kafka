package streamprocessing

import org.apache.spark.sql.SparkSession
import utils.DataFrameIOUtil
import utils.EncryptionUtil._
import org.apache.spark.sql.functions.{col, udf}

object StreamProcess {

  /**
    * Schema of data red from Kafka
    * root
    * |-- key: binary (nullable = true)
    * |-- value: binary (nullable = true)
    * |-- topic: string (nullable = true)
    * |-- partition: integer (nullable = true)
    * |-- offset: long (nullable = true)
    * |-- timestamp: timestamp (nullable = true)
    * |-- timestampType: integer (nullable = true)
    * */

  def process(sparkSession: SparkSession):Unit = {


    //TODO A lot lol.
    val inputDF = DataFrameIOUtil.readKafkaAsDataFrame(sparkSession,"topic",true)

    import sparkSession.implicits._
    //Using foreachBatch because
    sparkSession.udf.register("decrypt",decrypt(_))
    inputDF.writeStream.foreachBatch((batchDf,batchId) => {
      //batchDf.select($"key".cast(String),decrypt($"value"))
    })
  }


}
