package stockhlm.data.process

import org.apache.spark.sql.functions._
import scala.collection.Seq
import stockhlm.common.func._

class CreateTempDf extends SparkConfDetails {

  /*
   * create temperature dataframe and
   * temporary table pressure in Hive
   */

  def createTempDataframe(): Unit = {

    val scf = new RefineData with Serializable

    println("start")

    val dfTemp1 = scf.refineUrlData(1, "temp")
    val dfTemp2 = scf.refineUrlData(2, "temp")
    val dfTmp1961To2012 = scf.refineUrlData(3, "temp")
    val dfTmpManual2013 = scf.refineUrlData(4, "temp")
    val dfTmpAuto2013 = scf.refineUrlData(5, "temp")

    val dfTmp1756To1858Final = dfTemp1.withColumn("temp_max", lit("NAN")).withColumn("temp_min", lit("NAN")).withColumn("temp_mean", lit("NAN"))

    val dfTmp1859To1960Final = dfTemp2.withColumn("temp_mean", lit("NAN"))

    val dfUnionTemperature = Seq(dfTmp1756To1858Final, dfTmp1859To1960Final, dfTmp1961To2012, dfTmpManual2013, dfTmpAuto2013).reduce(_ union _)
     
     
     val optionsTemp = Map("path" -> "/stockhlm_temp")
    dfUnionTemperature.write.options(optionsTemp).saveAsTable("stockhlm.temperature")

    println("temperature table created")

  }
}
