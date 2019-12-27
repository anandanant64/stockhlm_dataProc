package stockhlm.common.func

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._

trait SparkConfDetails {

  lazy val spark = SparkSession
    .builder()
      .appName("stockhlm data process")
      .enableHiveSupport()
      .getOrCreate()
}