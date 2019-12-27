package stockhlm.data.process

import stockhlm.common.func._

class CreatePresDf extends SparkConfDetails {

  /*
   * create pressure dataframe and
   * temporary table pressure in Hive
   */

  def createTempDataframe(): Unit = {

    val scf = new RefineData with Serializable

    val dfPresHom = scf.refineUrlData(1, "pres")
    val dfPresNhom = scf.refineUrlData(2, "pres")
    val dfPresManual2013To2017 = scf.refineUrlData(3, "pres")
    val dfPres_Auto2013To2017 = scf.refineUrlData(4, "pres")

    val dfUnionPressure = Seq(dfPresHom, dfPresNhom, dfPresManual2013To2017, dfPres_Auto2013To2017).reduce(_ union _)

    println("Final pressure  DF is created")
    
    val optionsPres = Map("path" -> "/stockhlm_presure")
    dfUnionPressure.write.options(optionsPres).saveAsTable("stockhlm.pressure")

    println("pressure table created")

  }
}
