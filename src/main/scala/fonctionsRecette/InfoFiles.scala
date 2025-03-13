package fonctionsRecette
import fonctionsHdfs.UtilsHdfs._
import fonctionsUtils.UtilsFile._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.SizeEstimator
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types._
import java.io.File
import scala.io.Source

/**
 * Fonctions utiles pour les informations de fichier sous spark (utile en recette)
 */
object InfoFiles {

  /**
   * Rajoute le nom du fichier en lecture en lui-même dans une nouvelle colonne
   *
   * @param spark: SparkSession -
   * @param modeColumn: String - Permet de choisir entre l'écriture ou l affichage du dataframe
   * @param pathTarget: String - Chemin du repertoire ou du fichier à modifier
   * @param header: Boolean - si l'on veut le header ou non
   * @param delimiter: String - Symbole qui sépare les valeurs les unes des autres
   * @param format: String - Format du fichier ("csv", "orc", etc ...)
   * @param autoriseSousRep: Boolean - Autorise de listage dans les sous-repertoires (default = false)
   * @param listeCheminsExclus: Array[String] - Liste de chemins à exclure ou de répertoires à exclure (default = Empty)
   * @param listeMotifExclus : Array[String] - Liste de motifs à exclure (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @param listeMotifOnly : Array[String] - Liste de motifs à garder (seulement) (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @return Unit (rien)
   */

  def addNameColumn(spark : SparkSession,
                    modeColumn: String,
                    pathTarget: String,
                    header : Boolean,
                    pathDirectoryResults: String = "",
                    delimiter:String = "|",
                    format:String = "csv",
                    autoriseSousRep: Boolean =false,
                    listeCheminsExclus: Array[String] = Array[String](),
                    listeMotifExclus: Array[String] = Array("_nameAdded"),
                    listeMotifOnly:  Array[String] = Array[String]()): Unit ={

    val pathSizeFile = new Path(s"${pathTarget}")
    val listFiles = getDirsWithFiles(pathSizeFile,spark,autoriseSousRep = autoriseSousRep,listeCheminsExclus=listeCheminsExclus,listeMotifExclus=listeMotifExclus,listeMotifOnly=listeMotifOnly)

    if (modeColumn == "write"){
      listFiles.foreach(x => writeSingleFile(readFile(spark,header,x,delimiter,format,true),
        s"${x.split('.')(0)}_nameAdded.$format",
        spark.sparkContext,
        saveMode = "overwrite",
        format = format,
        delimiterCaractere = delimiter))
    }
    else if (modeColumn == "print"){
      listFiles.foreach(x => readFile(spark,header,x,delimiter,format,true).show())
    }
  }
  /** creerIndexDataFrame
   * Créer une colonne index pour un dataframe
   *
   * @param df : DataFrame - DataFrame
   * @return Dataframe: Contenant la colonne index
   */

  def creerIndexDataFrame(df : DataFrame) : DataFrame = {
    val name = "index"
    val rdd = df.rdd.zipWithIndex
      .map{ case (row, i) => Row.fromSeq(row.toSeq :+ i) }
    val newSchema = df.schema
      .add(StructField(name, LongType, false))
    df.sparkSession.createDataFrame(rdd, newSchema)
  }


  /** supprimerLignesDataFrame
   * Supprime des lignes d'un Dataframe
   *
   * @param df : DataFrame - DataFrame
   * @param nombreLignes : String - Nombre de lignes à supprimer
   * @return Dataframe: Contenant le nombre de lignes voulues supprimées
   */
  def supprimerLignesDataFrame(df : DataFrame, nombreLignes : Int) : DataFrame = {
    creerIndexDataFrame(df).where(col("index") > nombreLignes-1).drop("index")
  }

  /** getInfoFiles
   * Charge les infos des fichiers contenus dans un répertoire donné (recursive) dans un dataframe
   * (Taille de fichiers | Nombre de lignes | Nom des colonnes (Structure) )
   *
   * @param pathDirectory: String - Chemin du répertoire
   * @param paraSize: Boolean - charge ou non la taille des fichiers
   * @param paraCountRaws: Boolean - charge ou non le nombre de lignes des fichiers
   * @param paraColumns: Boolean - charge ou non les colonnes des fichiers
   * @param spark: SparkSession -
   * @param paraPath: Boolean - charge le chemin en entier ou non
   * @param delimiter: String - Symbole qui sépare les valeurs les unes des autres (default = "|")
   * @param format: String - Format du fichier ("csv", "orc", etc ...) (default = "csv")
   * @param autoriseSousRep: Boolean - Autorise de listage dans les sous-repertoires (default = false)
   * @param listeCheminsExclus: Array[String] - Liste de chemins à exclure ou de répertoires à exclure (default = Empty)
   * @param listeMotifExclus : Array[String] - Liste de motifs à exclure (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @param listeMotifOnly : Array[String] - Liste de motifs à garder (seulement) (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @return Dataframe: Contenant les infos de chaque fichier en fonction des paramètres
   */
  def getInfoFiles(pathDirectory: String,
                   paraSize:Boolean,
                   paraCountRaws:Boolean,
                   paraColumns: Boolean,
                   spark: SparkSession,
                   paraPath: Boolean = false,
                   delimiter:String = "|",
                   format:String = "csv",
                   autoriseSousRep:Boolean = false,
                   listeCheminsExclus: Array[String] = Array[String](),
                   listeMotifExclus: Array[String] = Array[String](),
                   listeMotifOnly:  Array[String] = Array[String]()): DataFrame ={

    import spark.implicits._
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val pathSizeFile = new Path(s"${pathDirectory}")
    val listFilesString = getDirsWithFiles(pathSizeFile,spark,autoriseSousRep=autoriseSousRep,listeCheminsExclus=listeCheminsExclus,listeMotifExclus=listeMotifExclus,listeMotifOnly=listeMotifOnly)

    val filesSize = scala.collection.mutable.Map[String, Long]()
    val filesCount = scala.collection.mutable.Map[String, Long]()
    val filesColumns = scala.collection.mutable.Map[String, String]()


    var df1 = spark.emptyDataFrame
    var df2 = spark.emptyDataFrame
    var df3 = spark.emptyDataFrame
    var resultDf = spark.emptyDataFrame
    val sc = spark.sparkContext



    if (paraPath == true){
      for (file <- listFilesString) {
        //val distFile = sc.textFile(file)
        val fileToRead = readFile(spark,true,file,delimiter,format)
        //val someFile = new File(file.split("file:").last)


        if (paraSize) filesSize += (file.split("/").last -> fs.getContentSummary(new Path(file)).getLength)
        //if (paraSize) filesSize += (file.split("/").last -> SizeEstimator.estimate(distFile))
        //if (paraSize) filesSize += (file.split("/").last -> someFile.length)

        if (paraCountRaws) filesCount +=  (file.split("/").last -> fileToRead.count())
        //if (paraCountRaws) filesCount +=  (file.split("/").last -> distFile.count)
        //if (paraCountRaws) filesCount +=  (file.split("/").last -> countLines(Source.fromFile(file.split("file:").last)))


        if (paraColumns) filesColumns += (file.split("/").last -> fileToRead.columns.mkString(" - "))
        //if (paraColumns) filesColumns += (file.split("/").last -> distFile.first())
        //if (paraColumns) filesColumns += (file.split("/").last -> Source.fromFile(file.split("file:").last).getLines().take(1).mkString)
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
   * @param delimiter: String - Symbole qui sépare les valeurs les unes des autres (default = "|")
   * @param format: String - Format du fichier ("csv", "orc", etc ...) (default = "csv")
   * @param autoriseSousRep: Boolean - Autorise de listage dans les sous-repertoires (default = false)
   * @param listeCheminsExclus: Array[String] - Liste de chemins à exclure ou de répertoires à exclure (default = Empty)
   * @param listeMotifExclus : Array[String] - Liste de motifs à exclure (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @param listeMotifOnly : Array[String] - Liste de motifs à garder (seulement) (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @return Unit (rien)
   */
  def printInfoFiles(pathDirectory: String,
                     paraSize:Boolean,
                     paraCountRaws:Boolean,
                     paraColumns: Boolean,
                     spark: SparkSession,
                     paraPath: Boolean = false,
                     paraGo:Boolean = false,
                     delimiter:String = "|",
                     format:String = "csv",
                     autoriseSousRep:Boolean = false,
                     listeCheminsExclus: Array[String] = Array[String](),
                     listeMotifExclus: Array[String] = Array[String](),
                     listeMotifOnly:  Array[String] = Array[String]()
                    ): Unit = {

    val df = getInfoFiles(pathDirectory, paraSize,paraCountRaws,paraColumns,spark,paraPath,delimiter=delimiter,format=format,autoriseSousRep = autoriseSousRep,listeCheminsExclus=listeCheminsExclus,listeMotifExclus=listeMotifExclus,listeMotifOnly=listeMotifOnly )

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
   * @param autoriseSousRep: Boolean - Autorise de listage dans les sous-repertoires (default = false)
   * @param listeCheminsExclus: Array[String] - Liste de chemins à exclure ou de répertoires à exclure (default = Empty)
   * @param listeMotifExclus : Array[String] - Liste de motifs à exclure (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @param listeMotifOnly : Array[String] - Liste de motifs à garder (seulement) (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @return Dataframe: Contenant la taille de chaque fichier
   */
  def getSizeFiles(pathDirectory: String,
                   spark:SparkSession,
                   autoriseSousRep:Boolean = false,
                   listeCheminsExclus: Array[String] = Array[String](),
                   listeMotifExclus: Array[String] = Array[String](),
                   listeMotifOnly:  Array[String] = Array[String]()):DataFrame = {
    import spark.implicits._
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val pathSizeFile = new Path(s"${pathDirectory}")
    val listFiles = getDirsWithFiles(pathSizeFile,spark,autoriseSousRep = autoriseSousRep,listeCheminsExclus=listeCheminsExclus,listeMotifExclus=listeMotifExclus,listeMotifOnly=listeMotifOnly)
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
   * @param autoriseSousRep: Boolean - Autorise de listage dans les sous-repertoires (default = false)
   * @param listeCheminsExclus: Array[String] - Liste de chemins à exclure ou de répertoires à exclure (default = Empty)
   * @param listeMotifExclus : Array[String] - Liste de motifs à exclure (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @param listeMotifOnly : Array[String] - Liste de motifs à garder (seulement) (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @return Unit (rien)
   */
  def printSizeFiles(pathDirectory: String,
                     spark: SparkSession,
                     paraGo:Boolean = false,
                     autoriseSousRep:Boolean = false,
                     listeCheminsExclus: Array[String] = Array[String](),
                     listeMotifExclus: Array[String] = Array[String](),
                     listeMotifOnly:  Array[String] = Array[String]()): Unit ={
    val df = getSizeFiles(pathDirectory, spark,autoriseSousRep = autoriseSousRep,listeCheminsExclus=listeCheminsExclus,listeMotifExclus=listeMotifExclus,listeMotifOnly=listeMotifOnly)

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
   * @param autoriseSousRep: Boolean - Autorise de listage dans les sous-repertoires (default = false)
   * @param listeCheminsExclus: Array[String] - Liste de chemins à exclure ou de répertoires à exclure (default = Empty)
   * @param listeMotifExclus : Array[String] - Liste de motifs à exclure (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @param listeMotifOnly : Array[String] - Liste de motifs à garder (seulement) (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @return Dataframe: Contenant le nombre de lignes de chaque fichier
   */
  def getRawCountFiles(pathDirectory: String,
                       spark: SparkSession,
                       delimiter:String = "|",
                       format:String = "csv",
                       autoriseSousRep:Boolean = false,
                       listeCheminsExclus: Array[String] = Array[String](),
                       listeMotifExclus: Array[String] = Array[String](),
                       listeMotifOnly:  Array[String] = Array[String]()): DataFrame ={
    import spark.implicits._
    val pathCountFile = new Path(s"${pathDirectory}")
    val listFiles = getDirsWithFiles(pathCountFile,spark,autoriseSousRep = autoriseSousRep,listeCheminsExclus=listeCheminsExclus,listeMotifExclus=listeMotifExclus,listeMotifOnly=listeMotifOnly)
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
   * @param autoriseSousRep: Boolean - Autorise de listage dans les sous-repertoires (default = false)
   * @param listeCheminsExclus: Array[String] - Liste de chemins à exclure ou de répertoires à exclure (default = Empty)
   * @param listeMotifExclus : Array[String] - Liste de motifs à exclure (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @param listeMotifOnly : Array[String] - Liste de motifs à garder (seulement) (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @return Unit (rien)
   */
  def printRawCountFiles(pathDirectory: String,
                         spark: SparkSession,
                         delimiter:String = "|",
                         format:String = "csv",
                         autoriseSousRep:Boolean = false,
                         listeCheminsExclus: Array[String] = Array[String](),
                         listeMotifExclus: Array[String] = Array[String](),
                         listeMotifOnly:  Array[String] = Array[String]()): Unit = {
    val df = getRawCountFiles(pathDirectory, spark,delimiter, format,autoriseSousRep = autoriseSousRep,listeCheminsExclus=listeCheminsExclus,listeMotifExclus=listeMotifExclus,listeMotifOnly=listeMotifOnly)
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
   * @param autoriseSousRep: Boolean - Autorise de listage dans les sous-repertoires (default = false)
   * @param listeCheminsExclus: Array[String] - Liste de chemins à exclure ou de répertoires à exclure (default = Empty)
   * @param listeMotifExclus : Array[String] - Liste de motifs à exclure (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @param listeMotifOnly : Array[String] - Liste de motifs à garder (seulement) (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @return Dataframe: Contenant le nom des colonnes de chaque fichier
   */
  def getColumnFiles(pathDirectory: String,
                     spark: SparkSession,
                     delimiter:String = "|",
                     format:String = "csv",
                     autoriseSousRep:Boolean = false,
                     listeCheminsExclus: Array[String] = Array[String](),
                     listeMotifExclus: Array[String] = Array[String](),
                     listeMotifOnly:  Array[String] = Array[String]()): DataFrame ={
    import spark.implicits._
    val pathCountFile = new Path(s"${pathDirectory}")
    val listFiles = getDirsWithFiles(pathCountFile,spark,autoriseSousRep = autoriseSousRep,listeCheminsExclus=listeCheminsExclus,listeMotifExclus=listeMotifExclus,listeMotifOnly=listeMotifOnly)
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
   * @param autoriseSousRep: Boolean - Autorise de listage dans les sous-repertoires (default = false)
   * @param listeCheminsExclus: Array[String] - Liste de chemins à exclure ou de répertoires à exclure (default = Empty)
   * @param listeMotifExclus : Array[String] - Liste de motifs à exclure (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @param listeMotifOnly : Array[String] - Liste de motifs à garder (seulement) (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @return Unit (rien)
   */
  def printColumnFiles(pathDirectory: String,
                       spark: SparkSession,
                       delimiter:String = "|",
                       format:String = "csv",
                       autoriseSousRep:Boolean = false,
                       listeCheminsExclus: Array[String] = Array[String](),
                       listeMotifExclus: Array[String] = Array[String](),
                       listeMotifOnly:  Array[String] = Array[String]()): Unit = {
    val df = getColumnFiles(pathDirectory, spark,delimiter,format,autoriseSousRep = autoriseSousRep,listeCheminsExclus=listeCheminsExclus,listeMotifExclus=listeMotifExclus,listeMotifOnly=listeMotifOnly)
    val dfToList = df.collect()

    println("Affichage des colonnes de fichiers")
    println("Fichier : Colonnes ")
    dfToList.foreach(row => {
      println(s"${row.getString(0)} \n ${row.get(1)}")
      println("-")
    })

  }

  //----------------------------------------------------------------------------------//

  /** writeInfoFiles
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
   * @param autoriseSousRep: Boolean - Autorise de listage dans les sous-repertoires (default = false)
   * @param listeCheminsExclus: Array[String] - Liste de chemins à exclure ou de répertoires à exclure (default = Empty)
   * @param listeMotifExclus : Array[String] - Liste de motifs à exclure (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @param listeMotifOnly : Array[String] - Liste de motifs à garder (seulement) (exemple: extension .csv etc... ou bien motif) (default = Empty)
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
                     format:String = "csv",
                     autoriseSousRep:Boolean = false,
                     listeCheminsExclus: Array[String] = Array[String](),
                     listeMotifExclus: Array[String] = Array[String](),
                     listeMotifOnly:  Array[String] = Array[String]()): Unit = {

    val df = getInfoFiles(pathDirectory, paraSize,paraCountRaws,paraColumns,spark,autoriseSousRep = autoriseSousRep,listeCheminsExclus=listeCheminsExclus,listeMotifExclus=listeMotifExclus,listeMotifOnly=listeMotifOnly)
    writeSingleFile(df,filename,spark.sparkContext,saveMode = saveMode,format =format,delimiterCaractere = delimiter)
  }
}
