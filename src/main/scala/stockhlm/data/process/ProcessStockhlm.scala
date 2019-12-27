package stockhlm.data.process

import stockhlm.common.func._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object ProcessStockhlm extends SparkConfDetails {

  def main(args: Array[String]) {

    //System.setProperty("hadoop.home.dir", "C://winutils");

    /*
    * Uncomment above if running on windows and download winutils
    * create dir C:\winutils\bin
    * and place winutils there
   */

    /*
    * drop database if exists
    * and create database stockhlm
    */

    spark.sql("drop database if exists stockhlm CASCADE")
    spark.sql("create database stockhlm")

    val ctdf = new CreateTempDf
    ctdf.createTempDataframe()

    val psdf = new CreatePresDf
    psdf.createTempDataframe()

  }

}