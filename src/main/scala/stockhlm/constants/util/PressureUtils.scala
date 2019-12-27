package stockhlm.constants.util

import org.apache.spark.sql.types._

class PressureUtils {

  val urlLinkP1 = "https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/stockholm_SLP_1756_2012_hPa_hom.txt"
  val urlLinkP2 = "https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/stockholm_SLP_1756_2012_hPa_nonhom.txt"
  val urlLinkP3 = "https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_2013_2017.txt"
  val urlLinkP4 = "https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholmA_barometer_2013_2017.txt"

  
  val pressureSeriesP = StructType(
    List(
      StructField("year", StringType, true),
      StructField("month", StringType, true),
      StructField("day", StringType, true),
      StructField("morn_reading", StringType, true),
      StructField("noon_reading", StringType, true),
      StructField("evng_reading", StringType, true)))
      
      // setter-getter for class variables
  
  def geturl(urlPressureNo: Int): String = urlPressureNo match {

    case 1 => return (s"$urlLinkP1")
    case 2 => return (s"$urlLinkP2")
    case 3 => return (s"$urlLinkP3")
    case 4 => return (s"$urlLinkP4")
   

  }
  
   def getSchema(): StructType =  {

    pressureSeriesP
  }
}