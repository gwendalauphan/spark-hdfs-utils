package fonctionsUtils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Fonctions utiles générales pour spark
 */
object UtilsSpark {

  /** creerSparkSession
   * Lit un fichier en entrée et le retourne en dataframe
   *
   * @param repertoireBaseSpark: String - Chemin de la base Spark
   * @param sessionName: String - Nom de session Spark
   * @return SparkSession
   */
  def creerSparkSession(repertoireBaseSpark: String, sessionName: String): SparkSession = {

    val sparkConf = new SparkConf().
      setIfMissing("spark.master", "local[*]").
      set("spark.sql.warehouse.dir", repertoireBaseSpark)
    
    val spark = SparkSession.builder.
      appName(sessionName).
      config(sparkConf).
      getOrCreate()

    spark
  }

  /** creerEtSetBasePardefaut
   * Créer une database si elle n'existe pas
   *
   * @param BASE: String - Nom de la base
   * @return Unit (rien)
   */
  def creerEtSetBasePardefaut(BASE: String): Unit = {

    //récupération de la sparkSession
    val spark: SparkSession = SparkSession.builder().getOrCreate()
    // creation de la base de donnees si besoin
    if (!spark.catalog.databaseExists(BASE))
      spark.sql(s"CREATE DATABASE $BASE")
    //set de la base par défaut
    spark.sql(s"use $BASE")
  }



}
