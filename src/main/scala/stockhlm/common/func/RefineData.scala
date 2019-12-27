package stockhlm.common.func

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import stockhlm.constants.util._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class RefineData extends SparkConfDetails {

  import spark.implicits._

  val reg = """[\t\p{Zs}]+"""
  val logger = Logger.getLogger(this.getClass.getName)
  val tmu = new TemperatureUtils
  val pus = new PressureUtils

  /*
   * remove multiple whitespace with comma as delimiter
   */

  val finalRefine = (x: String) => (x.trim).replaceAll(reg, ",")

  /*
   * get specific temperature or pressure url
   */

  def urlLink(n: Int, x: String): String = x match {
    case "temp" => tmu.getUrl(n)
    case "pres" => pus.geturl(n)
  }

  /*
  * create RDD of row type as per schema defined
  */

  def createRowRdd(m: Int, passRdd: RDD[String]): RDD[Row] = {

    val rdd_df_init = passRdd.map(x => Row.fromSeq(x.split(",")))

    rdd_df_init

  }

  /*
   * get url of either temp or pressure from util class
   * filter blank lines and create rdd
   * call createRowRdd and create RDD[Row]
   * use createDataFrame to create df using RDD[row created and Schema defined in Utils class
   */

  def refineUrlData(urlNumber: Int, urlType: String): DataFrame = {
    logger.info(s"value of urlNumber is : $urlNumber")

    logger.info("Process starts")

    val urlLink$urlNumber = urlLink(urlNumber, urlType)
    logger.info("url fetched")

    val html$urlNumber = scala.io.Source.fromURL(urlLink$urlNumber).mkString
    logger.info("url string created")

    val list$urlNumber = html$urlNumber.split("\n").filter(_ != "")
    logger.info("url data split done and filtered")

    val rdds$urlNumber = spark.sparkContext.parallelize(list$urlNumber)
    logger.info("rdd created")

    val rddsFinal$urlNumber = rdds$urlNumber.map(finalRefine)
    logger.info("row mapped with records")

    if (urlType == "temp") {
      val df$urlNumber = spark.createDataFrame(createRowRdd(urlNumber, rddsFinal$urlNumber), tmu.getSchema(urlNumber))
      logger.info(s"Temperature process completed for temp file $urlNumber")
      df$urlNumber
    } else {
      val df$urlNumber = spark.createDataFrame(createRowRdd(urlNumber, rddsFinal$urlNumber), pus.getSchema())
      logger.info(s"Prsessure process completed for pressure file $urlNumber")
      df$urlNumber
    }
  }

}