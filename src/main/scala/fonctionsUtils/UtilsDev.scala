package fonctionsUtils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

object UtilsDev {

  /** compareRegex
   * Extrait la date en string des noms de fichiers csv
   *
   * @param schema: String - variable string a traiter
   * @param formatRegex: String - Format Regex a extraire
   * @return String - Resultat de l extraction
   */
  def compareRegex(schema: String, formatRegex: String): String ={    //Extrait la date en string des noms de fichiers csv
    val regex = formatRegex.r
    try {
      val regex(resultat) = schema
      resultat
    }
    catch {
      case matcherror: MatchError =>
        println(s"MatchError Exception occurred with ${schema}")
        ""
    }
  }

  /** biggestSizeFile
   *
   * @param listeFiles: Array[String] - Liste des fichiers a traiter
   * @param spark: SparkSession -
   * @return String - Renvoie le fichier le plus gros parmis la liste de fichiers
   */
  def biggestSizeFile(listeFiles: Array[String], spark:SparkSession ): String ={ // Renvoie le fichier le plus gros parmis une liste de fichiers
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    var tmp: Long = 0
    var result: String = ""
    for (file <- listeFiles){
      if (fs.getContentSummary(new Path(file)).getLength > tmp){
        result = file
        tmp = fs.getContentSummary(new Path(file)).getLength
      }
    }
    result
  }

  /** mostRecentDateFile
   *
   * @param listeFiles: Array[String] - Liste des fichiers a traiter
   * @param spark: SparkSession -
   * @return String -  Renvoie le nom fichier le plus récent parmis la liste
   */
  def mostRecentDateFile(listeFiles: Array[String], spark:SparkSession ): String = { // Renvoie le fichier le plus récent parmis une liste
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    var tmp: Long = 0
    var result: String = ""
    for (file <- listeFiles) {
      if (fs.getFileStatus(new Path(file)).getModificationTime() > tmp) {
        result = file
        tmp = fs.getFileStatus(new Path(file)).getModificationTime()
      }
    }
    result
  }

  /** compareDates
   *
   * @param date1: String - date1 yyyy-mm-dd
   * @param date2: String - date2 yyyy-mm-dd
   * @return Boolean - vrai si la date1 est plus recente a date2 et faux dans le cas contraire
   */
  def compareDates(date1: String, date2: String): Boolean = {   //Compare 2 dates
    val resultat = date1.compareTo(date2)
    if (resultat <= 0) {false}
    else {true}
  }

}
