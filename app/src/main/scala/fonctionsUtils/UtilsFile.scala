package fonctionsUtils

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import scala.io.Source

/**
 * Fonctions utiles au développement liés aux fichiers sous spark
 */
object UtilsFile {

  /** readFile
   * Lit un fichier en entrée et le retourne en dataframe
   *
   * @param spark: SparkSession -
   * @param header: Boolean - si l'on veut le header ou non
   * @param path: String - Chemin du fichier à lire
   * @param delimiter: String - Symbole qui sépare les valeurs les unes des autres
   * @param format: String - Format du fichier ("csv", "orc", etc ...)
   * @param addName: Boolean - Variable qui determine l'ecriture du nom du fichier lu dans ce dernier
   * @return Dataframe avec le contenu du fichier
   */

  def readFile (spark : SparkSession, header : Boolean, path: String, delimiter : String, format : String, addName: Boolean = false) : DataFrame = {

    val df = spark.read.format(format).
      option("delimiter",delimiter).
      option("header",header).
      option("inferSchema", "true").
      // option("nullValue","null").
      load(path)

    if (addName ==true) {
      df.withColumn("Nom de fichier", lit(path.split("/").last))
    }
    else df

  }


  /** writeSingleFile
   * Ecrit le contenu d'un dataframe dans un fichier
   *
   * @param df: Dataframe en entrée
   * @param filename: String - Chemin + nom de fichier en sortie
   * @param sc: SparkContext - (ex: spark.sparkContext)
   * @param tmpFolder: String - Nom de fichier temporaire (default = "tmp_folder")
   * @param saveMode: String - Regarde si le fichier existe déjà (default = "error")
   * @param format: String - Format du fichier ("csv", "orc", etc ...) (default = "csv")
   * @param delimiterCaractere: String - Symbole qui sépare les valeurs les unes des autres (default = "|")
   * @return Unit (rien)
   */
  def writeSingleFile(
                       df: DataFrame,             // must be small
                       filename: String,          // the full filename you want outputted
                       sc: SparkContext,          // pass in spark.sparkContext
                       tmpFolder: String = "tmp_folder",         // will be deleted, so make sure it doesn't already exist
                       saveMode: String = "error", // Spark default is error, overwrite and append are also common
                       format: String = "csv",    // csv, parquet
                       delimiterCaractere:String = "|" 
                     ): Unit = {
    df.repartition(1)
      .write
      .mode(saveMode)
      .option("header","true")
      .option("delimiter",delimiterCaractere)
      .format(format)
      .save(tmpFolder)


    val conf    = sc.hadoopConfiguration
    val src     = new Path(tmpFolder)
    val fs      = src.getFileSystem(conf)
    val oneFile = fs.listStatus(src).map(x => x.getPath.toString()).find(x => x.endsWith(format))
    val srcFile = new Path(oneFile.getOrElse(""))
    val dest    = new Path(filename)
    val tmp = new Path(tmpFolder)
    if(fs.exists(dest) && fs.isFile(dest))
      fs.delete(dest,true)
    fs.rename(srcFile, dest)
    if(fs.exists(tmp) && fs.isDirectory(tmp))
      fs.delete(tmp,true)

  }

  /** controleColonnesDf
   *
   * @param df: dataframe -
   * @param listColumns: List[String] - Liste des colonnes a verifier
   * @return Boolean (vrai si la liste de colonne est bonne et non si ce n est pas le cas)
   */

  def controleColonnesDf(df : DataFrame, listColumns : List[String]): Boolean ={
    listColumns.map(
      a => if(!df.columns.contains(a)) {
        return false
      }
    )
    true
  }

  /** countLines
   *
   * @param source: Source - iterable representation
   * @return Long - Nombre de lignes
   */

  def countLines(source: Source): Long = {
    var newlineCount = 0L
    for (line <- source.getLines) {
      newlineCount += 1
    }
    newlineCount
  }
}


