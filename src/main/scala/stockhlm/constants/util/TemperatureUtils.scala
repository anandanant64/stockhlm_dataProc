package stockhlm.constants.util

import org.apache.spark.sql.types._

class TemperatureUtils {

  val temperatureSeriesT1 = StructType(
    List(
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true),
      StructField("morn_reading", StringType, true),
      StructField("noon_reading", StringType, true),
      StructField("evng_reading", StringType, true)))

  val temperatureSeriesT2 = StructType(
    List(
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true),
      StructField("morn_reading", StringType, true),
      StructField("noon_reading", StringType, true),
      StructField("evng_reading", StringType, true),
      StructField("temp_max", StringType, true),
      StructField("temp_min", StringType, true)))

  val temperatureSeriesT = StructType(
    List(
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true),
      StructField("morn_reading", StringType, true),
      StructField("noon_reading", StringType, true),
      StructField("evng_reading", StringType, true),
      StructField("temp_max", StringType, true),
      StructField("temp_min", StringType, true),
      StructField("temp_mean", StringType, true)))

  val urlLinkT1 = "https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt"
  val urlLinkT2 = "https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt"
  val urlLinkT3 = "https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt"
  val urlLinkT4 = "https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt"
  val urlLinkT5 = "https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt"

  
  // setter-getter for class variables
  
  def getUrl(urlTempNo: Int): String = urlTempNo match {

    case 1 => return (s"$urlLinkT1")
    case 2 => return (s"$urlLinkT2")
    case 3 => return (s"$urlLinkT3")
    case 4 => return (s"$urlLinkT4")
    case 5 => return (s"$urlLinkT5")

  }
  
  def getSchema(tempSchemaNo: Int): StructType = tempSchemaNo match {

    case 1 => return (temperatureSeriesT1)
    case 2 => return (temperatureSeriesT2)
    case _ => return (temperatureSeriesT)

  }

}