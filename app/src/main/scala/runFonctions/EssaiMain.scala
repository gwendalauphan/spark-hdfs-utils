package runFonctions

import fonctionsRecette.InfoFiles.{addNameColumn, writeInfoFiles}
import fonctionsUtils.UtilsSpark.{creerEtSetBasePardefaut, creerSparkSession}
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger

object EssaiMain {

  val logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    ////////////////////////////////////////////
    /// CONF : Gestion des variables chemins ///
    ////////////////////////////////////////////

    
    val rootPath = "/app/user/spark"
    //get paths
    val repertoireBaseSpark = if (args.length > 0) args(0) else s"$rootPath/repertoireBaseSpark"
    val baseSparkName = if (args.length > 1) args(1) else "baseSpark"
    val pathData = if (args.length > 2) args(2) else s"$rootPath/pathData"
    val pathResults = if (args.length > 3) args(3) else s"$rootPath/pathResults"

    var tempsMethode = scala.collection.mutable.Map[String, Long]()

    //Creation de la Session
    val spark = creerSparkSession(repertoireBaseSpark, baseSparkName)
    logger.info("Creation de la session Spark de l'application")

    //Creation de la base de donnees
    logger.info("Creation de la DATABASE ")
    creerEtSetBasePardefaut(baseSparkName)
    val pathSizeFile = new Path(s"${pathData}")


    val tempInit = "2020-01-01"
    val tempInit2 ="essai"
    //addNameColumn(spark, modeColumn = "print", pathData, true, pathResults, "|", "csv")
    //val dfEssai = fonctionsHdfs.UtilsHdfs.getDirsWithFiles(pathSizeFile,spark,true,listeMotifOnly = Array("stats_log.csv","stats_etat.csv",tempInit,tempInit2) )

    //fonctionsRecette.InfoFiles.writeInfoFiles(pathData,pathData+"/infos_files",true,true,true,spark,listeMotifOnly = Array("stats_log.csv","stats_etat.csv",tempInit,tempInit2),format = "csv",autoriseSousRep = true)

    //dfEssai.foreach(println)


    //val listFilepath = getDirsWithFiles(pathSizeFile,spark,autoriseSousRep = true)
    //listFilepath.foreach(println)

    /*
    val listeMethode = List("readFile","textFile","newFile")

    for (methode <- listeMethode){
      var t0 = current_timestamp()
      getInfoFiles(pathData,true,false,false,spark,false,methode)
      var delta = current_timestamp() - t0
      tempsMethode += (methode, delta)

    }*/


    logger.info("Fin de la session")
    spark.stop()
  }

}
