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
    println("Hello Jérôme")

    //get paths
    val repertoireBaseSpark = args(0) //"/projets/psn/test-fonctions_recette/base",
    val baseSpark = args(1) //"fonctions_recette"
    val pathDirectory = args(2) //"/projets/psn/test-fonctions_recette/donnees_test"
    val pathDirectoryResults = args(3) //"/projets/psn/test-fonctions_recette/resultats"

    var tempsMethode = scala.collection.mutable.Map[String, Long]()

    //Creation de la Session
    val spark = creerSparkSession(repertoireBaseSpark, baseSpark)
    logger.info("Creation de la session Spark de l'application")

    //Creation de la base de donnees
    logger.info("Creation de la DATABASE ")
    creerEtSetBasePardefaut(baseSpark)
    val pathSizeFile = new Path(s"${pathDirectory}")


    val tempInit = "2020-01-01"
    val tempInit2 ="essai"
    //addNameColumn(spark, modeColumn = "print", pathDirectory, true, pathDirectoryResults, "|", "csv")
    //val dfEssai = fonctionsHdfs.UtilsHdfs.getDirsWithFiles(pathSizeFile,spark,true,listeMotifOnly = Array("stats_log.csv","stats_etat.csv",tempInit,tempInit2) )

    //fonctionsRecette.InfoFiles.writeInfoFiles(pathDirectory,pathDirectory+"/infos_files",true,true,true,spark,listeMotifOnly = Array("stats_log.csv","stats_etat.csv",tempInit,tempInit2),format = "csv",autoriseSousRep = true)

    //dfEssai.foreach(println)


    //val listFilepath = getDirsWithFiles(pathSizeFile,spark,autoriseSousRep = true)
    //listFilepath.foreach(println)

    /*
    val listeMethode = List("readFile","textFile","newFile")

    for (methode <- listeMethode){
      var t0 = current_timestamp()
      getInfoFiles(pathDirectory,true,false,false,spark,false,methode)
      var delta = current_timestamp() - t0
      tempsMethode += (methode, delta)

    }*/


    logger.info("Fin de la session")
    spark.stop()
  }

}
