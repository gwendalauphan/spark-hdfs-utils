package fonctionsRecette
import fonctionsHdfs.UtilsHdfs._
import fonctionsUtils.UtilsFile._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.SizeEstimator

import java.io.File
import scala.io.Source

/**
 * Fonctions utiles pour les informations de fichier sous spark (utile en recette)
 */
object InfoFilesbis {

  /** getInfoFiles
   * Charge les infos des fichiers contenus dans un répertoire donné (recursive) dans un dataframe
   * (Taille de fichiers | Nombre de lignes | Nom des colonnes (Structure)
   *
   * @param pathDirectory: String - Chemin du répertoire
   * @param paraSize: Boolean - charge ou non la taille des fichiers
   * @param paraCountRaws: Boolean - charge ou non le nombre de lignes des fichiers
   * @param paraColumns: Boolean - charge ou non les colonnes des fichiers
   * @param spark: SparkSession -
   * @param paraPath: Boolean - charge le chemin en entier ou non
   * @param paraOpti: String - Determine la méthode de calcul
   * @param delimiter: String - Symbole qui sépare les valeurs les unes des autres (default = "|")
   * @param format: String - Format du fichier ("csv", "orc", etc ...) (default = "csv")
   * @return Dataframe: Contenant les infos de chaque fichier en fonction des paramètres
   */
  def getInfoFiles(pathDirectory: String,
                   paraSize:Boolean,
                   paraCountRaws:Boolean,
                   paraColumns: Boolean,
                   spark: SparkSession,
                   paraPath: Boolean = false,
                   paraOpti: String = "readFile",
                   delimiter:String = "|",
                   format:String = "csv"): DataFrame ={

    import spark.implicits._
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val pathSizeFile = new Path(s"${pathDirectory}")
    val listFiles = getDirsWithFiles(pathSizeFile,spark)
    val listFilesString = listFiles.map(_.toString) //Format en String

    val filesSize = scala.collection.mutable.Map[String, Long]()
    val filesCount = scala.collection.mutable.Map[String, Long]()
    val filesColumns = scala.collection.mutable.Map[String, String]()


    var df1 = spark.emptyDataFrame
    var df2 = spark.emptyDataFrame
    var df3 = spark.emptyDataFrame
    var resultDf = spark.emptyDataFrame
    val sc = spark.sparkContext



    if (paraPath == true){
      if (paraOpti == "readFile"){
        for (file <- listFilesString) {
          val fileToRead = readFile(spark,true,file,delimiter,format)

          if (paraSize) filesSize += (file.split("/").last -> fs.getContentSummary(new Path(file)).getLength)
          if (paraCountRaws) filesCount +=  (file.split("/").last -> fileToRead.count())
          if (paraColumns) filesColumns += (file.split("/").last -> fileToRead.columns.mkString(" - "))

        }
      }
      else if (paraOpti == "textFile"){
        for (file <- listFilesString) {
          val distFile = sc.textFile(file)

          if (paraSize) filesSize += (file.split("/").last -> SizeEstimator.estimate(distFile))
          if (paraCountRaws) filesCount +=  (file.split("/").last -> distFile.count)
          if (paraColumns) filesColumns += (file.split("/").last -> distFile.first())

        }
      }
      else if (paraOpti == "newFile"){
        for (file <- listFilesString) {

          val someFile = new File(file.split("file:").last)

          if (paraSize) filesSize += (file.split("/").last -> someFile.length)
          if (paraCountRaws) filesCount +=  (file.split("/").last -> countLines(Source.fromFile(file.split("file:").last)))
          if (paraColumns) filesColumns += (file.split("/").last -> Source.fromFile(file.split("file:").last).getLines().take(1).mkString)

        }
      }

      resultDf = listFilesString.map(_.split("/").last).toList.toDF("fileName")
    }

    else if (paraPath == false){
      for (file <- listFilesString) {
        if (paraSize) filesSize += (file -> fs.getContentSummary(new Path(file)).getLength)
        val fileToRead = readFile(spark,true,file,delimiter,format)
        if (paraCountRaws) filesCount +=  (file -> fileToRead.count())
        if (paraColumns) filesColumns += (file -> fileToRead.columns.mkString(" - "))
      }
      resultDf = listFilesString.toList.toDF("fileName")
    }

    if (paraSize) {
      df1 = filesSize.toSeq.toDF("fileName", "fileSize")
      resultDf = resultDf.join(df1, Seq("filename"))
    }
    if (paraCountRaws){
      df2 = filesCount.toSeq.toDF("fileName", "rowCount")
      resultDf = resultDf.join(df2, Seq("filename"))
    }
    if (paraColumns) {
      df3 = filesColumns.toSeq.toDF("fileName", "columnsName")
      resultDf = resultDf.join(df3, Seq("filename"))
    }
    resultDf

  }

  /** printInfoFiles
   * Affiche les infos de fichiers contenus dans un répertoire donné (recursive)
   * (Taille de fichiers | Nombre de lignes | Nom des colonnes (Structure)
   * (Affiche le contenu de getInfoFiles) dans le terminal
   *
   * @param pathDirectory: String - Chemin du répertoire
   * @param paraSize: Boolean - affiche ou non la taille des fichiers
   * @param paraCountRaws: Boolean - affiche ou non le nombre de lignes des fichiers
   * @param paraColumns: Boolean - affiche ou non les colonnes des fichiers
   * @param spark: SparkSession -
   * @param paraPath: Boolean - charge le chemin en entier ou non
   * @param paraGo: Boolean - affiche ou non la taille en Go (default = false)
   * @return Unit (rien)
   */
  def printInfoFiles(pathDirectory: String,
                     paraSize:Boolean,
                     paraCountRaws:Boolean,
                     paraColumns: Boolean,
                     spark: SparkSession,
                     paraPath: Boolean = false,
                     paraGo:Boolean = false
                    ): Unit = {

    val df = getInfoFiles(pathDirectory, paraSize,paraCountRaws,paraColumns,spark,paraPath)

    val listeColumns = df.columns
    val dfToList = df.collect()
    var listePhrases: Array[String] = Array("NameFile | ")
    for (c <- listeColumns){
      if (c == "fileSize") {
        if (paraGo == false){
          listePhrases :+= "octets | "
        }
        else if (paraGo == true) {
          listePhrases :+= "octets |& Go |"
        }
      }
      else if (c == "rowCount") {
        listePhrases :+= "ligne(s) | "
      }
      else if (c == "columnsName"){
        listePhrases :+= " | NameColumn(s) |"
      }
    }
    var i = 0

    dfToList.foreach(row => {
      for (column <- listeColumns){
        i = listeColumns.indexOf(column)
        if (listePhrases(i) == " | NameColumn(s) |") println("")
        if (listePhrases(i) != "octets |& Go |") print(s"${row.get(i)} ${listePhrases(i)}")
        else {
          print(s"${row.get(i)} ${listePhrases(i).split("&")(0)}")
          print(s"${BigDecimal(row.getLong(i).toFloat/1073741824).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble} ${listePhrases(i).split("&")(1)}")
        }
      }
      println("")
      println("-")
    })

  }

  //----------------------------------------------------------------------------------//

  /** getSizeFiles
   * Charge la taille des fichiers contenus dans un répertoire donné (recursive) dans un dataframe
   *
   * @param pathDirectory: String - Chemin du répertoire
   * @param spark: SparkSession -
   * @return Dataframe: Contenant la taille de chaque fichier
   */
  def getSizeFiles(pathDirectory: String, spark:SparkSession):DataFrame = {
    import spark.implicits._
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val pathSizeFile = new Path(s"${pathDirectory}")
    val listFiles = getDirsWithFiles(pathSizeFile,spark)
    val listFilesString = listFiles.map(_.toString) //Format en String

    val filesSize = scala.collection.mutable.Map[String, Long]()
    for (file <- listFilesString) {
      filesSize += (file -> fs.getContentSummary(new Path(file)).getLength)
    }

    val df = filesSize.toSeq.toDF("fileName", "fileSize").sort(col("fileSize").desc)
    df
  }

  /** printSizeFiles
   * Affiche la taille des fichiers contenus dans un répertoire donné (recursive)
   * (Affiche le contenu de getSizeFiles) dans le terminal
   *
   * @param pathDirectory: String - Chemin du répertoire
   * @param spark: SparkSession -
   * @param paraGo: Boolean - affiche ou non la taille en Go (default = false)
   * @return Unit (rien)
   */
  def printSizeFiles(pathDirectory: String,spark: SparkSession, paraGo:Boolean = false): Unit ={
    val df = getSizeFiles(pathDirectory, spark)

    val dfToList = df.collect()

    println("Taille des fichiers")
    println("Fichier : Taille (octets)")
    if (paraGo == false) {
      dfToList.foreach(row => {
        println(s"${row.getString(0)}: ${row.getLong(1)} octets")
      })
    }
    else if (paraGo == true) {
      dfToList.foreach(row => {
        println(s"${row.getString(0)}: ${row.getLong(1)} octets |  ${BigDecimal(row.getLong(1).toFloat/1073741824).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble} Go")
      })
    }
  }

  //----------------------------------------------------------------------------------//

  /** getRawCountFiles
   * Charge le nombre de lignes des fichiers contenus dans un répertoire donné (recursive) dans un dataframe
   *
   * @param pathDirectory: String - Chemin du répertoire
   * @param spark: SparkSession -
   * @param delimiter: String - Symbole qui sépare les valeurs les unes des autres (default = "|")
   * @param format: String - Format du fichier ("csv", "orc", etc ...) (default = "csv")
   * @return Dataframe: Contenant le nombre de lignes de chaque fichier
   */
  def getRawCountFiles(pathDirectory: String,
                       spark: SparkSession,
                       delimiter:String = "|",
                       format:String = "csv"): DataFrame ={
    import spark.implicits._
    val pathCountFile = new Path(s"${pathDirectory}")
    val listFiles = getDirsWithFiles(pathCountFile,spark)
    val listFilesString = listFiles.map(_.toString) //Format en String

    val filesCount = scala.collection.mutable.Map[String, Long]()
    for (file <- listFilesString){
      filesCount +=  (file -> readFile(spark,true,file,delimiter,format).count())
    }

    val df = filesCount.toSeq.toDF("fileName", "rowCount").sort(col("rowCount").desc)
    df
  }

  /** printRawCountFiles
   * Affiche le nombre de lignes des fichiers contenus dans un répertoire donné (recursive)
   * (Affiche le contenu de getRawCountFiles) dans le terminal
   *
   * @param pathDirectory: String - Chemin du répertoire
   * @param spark: SparkSession -
   * @param delimiter: String - Symbole qui sépare les valeurs les unes des autres (default = "|")
   * @param format: String - Format du fichier ("csv", "orc", etc ...) (default = "csv")
   * @return Unit (rien)
   */
  def printRawCountFiles(pathDirectory: String,spark: SparkSession,delimiter:String = "|", format:String = "csv"): Unit = {
    val df = getRawCountFiles(pathDirectory, spark,delimiter, format)
    val dfToList = df.collect()

    println("Nombres de lignes des fichiers")
    println("Fichier : Nb lignes ")
    dfToList.foreach(row => {
      println(s"${row.getString(0)}: ${row.getLong(1)} ligne(s)")
    })

  }

  //----------------------------------------------------------------------------------//

  /** getColumnFiles
   * Charge le nom des colonnes des fichiers contenus dans un répertoire donné (recursive) dans un dataframe
   *
   * @param pathDirectory: String - Chemin du répertoire
   * @param spark: SparkSession -
   * @param delimiter: String - Symbole qui sépare les valeurs les unes des autres (default = "|")
   * @param format: String - Format du fichier ("csv", "orc", etc ...) (default = "csv")
   * @return Dataframe: Contenant le nom des colonnes de chaque fichier
   */
  def getColumnFiles(pathDirectory: String,
                     spark: SparkSession,
                     delimiter:String = "|",
                     format:String = "csv"): DataFrame ={
    import spark.implicits._
    val pathCountFile = new Path(s"${pathDirectory}")
    val listFiles = getDirsWithFiles(pathCountFile,spark)
    val listFilesString = listFiles.map(_.toString) //Format en String

    val filesColumns = scala.collection.mutable.Map[String, Array[String]]()
    for (file <- listFilesString) {
      filesColumns += (file -> readFile(spark,true,file,delimiter,format).columns)
    }
    val df = filesColumns.toSeq.toDF("fileName", "columnsName")
    df
  }

  /** printColumnFiles
   * Affiche le nom des colonnes des fichiers contenus dans un répertoire donné (recursive)
   * (Affiche le contenu de getRawCountFiles) dans le terminal
   *
   * @param pathDirectory: String - Chemin du répertoire
   * @param spark: SparkSession -
   * @param delimiter: String - Symbole qui sépare les valeurs les unes des autres (default = "|")
   * @param format: String - Format du fichier ("csv", "orc", etc ...) (default = "csv")
   * @return Unit (rien)
   */
  def printColumnFiles(pathDirectory: String,spark: SparkSession,delimiter:String = "|", format:String = "csv"): Unit = {
    val df = getColumnFiles(pathDirectory, spark,delimiter,format)
    val dfToList = df.collect()

    println("Affichage des colonnes de fichiers")
    println("Fichier : Colonnes ")
    dfToList.foreach(row => {
      println(s"${row.getString(0)} \n ${row.get(1)}")
      println("-")
    })

  }

  //----------------------------------------------------------------------------------//

  /** writeSingleFile
   * Ecrit les infos des fichiers contenus dans un répertoire donné (recursive) dans un fichier
   * (Taille de fichiers | Nombre de lignes | Nom des colonnes (Structure)
   * Ecrit le contenu de getInfoFiles dans un fichier
   *
   * @param pathDirectory: String - Chemin du répertoire
   * @param filename: String - Chemin + nom de fichier en sortie
   * @param paraSize: Boolean - ecrit ou non la taille des fichiers
   * @param paraCountRaws: Boolean - ecrit ou non le nombre de lignes des fichiers
   * @param paraColumns: Boolean - ecrit ou non les colonnes des fichiers
   * @param spark: SparkSession -
   * @param paraPath: Boolean - charge le chemin en entier ou non
   * @param saveMode: String - Regarde si le fichier existe déjà (default = "overwrite")
   * @param delimiter: String - Symbole qui sépare les valeurs les unes des autres (default = "|")
   * @param format: String - Format du fichier ("csv", "orc", etc ...) (default = "csv")
   * @return Unit (rien)
   */
  def writeInfoFiles(pathDirectory: String,
                     filename: String,
                     paraSize:Boolean,
                     paraCountRaws:Boolean,
                     paraColumns: Boolean,
                     spark: SparkSession,
                     paraPath: Boolean = false,
                     saveMode: String = "overwrite",
                     delimiter:String = "|",
                     format:String = "csv"): Unit = {

    val df = getInfoFiles(pathDirectory, paraSize,paraCountRaws,paraColumns,spark)
    writeSingleFile(df,filename,spark.sparkContext,saveMode = saveMode,format =format,delimiterCaractere = delimiter)
  }
}
