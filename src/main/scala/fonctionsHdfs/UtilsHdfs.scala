package fonctionsHdfs

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
  * Fonctions dédiées à l'utilisation hadoop hdfs sous spark
*/
object UtilsHdfs {

  /** getDirsWithFiles
   * Retourne la liste de chemins contenu dans le répertoire (recursive)
   *
   * @param hdfsPath: Path - chemin du répertoire ou du fichier
   * @param spark: SparkSession -
   * @param autoriseSousRep: Boolean - Autorise de listage dans les sous-repertoires (default = false)
   * @param listeCheminsExclus: Array[String] - Liste de chemins à exclure ou de répertoires à exclure (default = Empty)
   * @param listeMotifExclus : Array[String] - Liste de motifs à exclure (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @param listeMotifOnly : Array[String] - Liste de motifs à garder (seulement) (exemple: extension .csv etc... ou bien motif) (default = Empty)
   * @return Array(Paths) du répertoire donné
   */
  def getDirsWithFiles(
                        hdfsPath: Path,
                        spark:SparkSession,
                        autoriseSousRep:Boolean = false,
                        listeCheminsExclus: Array[String] = Array[String](),
                        listeMotifExclus: Array[String] = Array[String](),
                        listeMotifOnly:  Array[String] = Array[String]()
                      ): Array[String] = { //Liste les fichiers d'un répertoire donné
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val listStatus = fs.listStatus(hdfsPath)
    if (autoriseSousRep) {


      if (fs.isDirectory(hdfsPath)) {
        val newListStatus = listStatus.filterNot(x => listeCheminsExclus.exists(y => y.matches(x.getPath.toString.split(":").last)))
        newListStatus.flatMap(innerPath => getDirsWithFiles(innerPath.getPath, spark,autoriseSousRep, listeCheminsExclus,listeMotifExclus,listeMotifOnly))
      } else {
        val listArray = Array(hdfsPath.toString)
        val listArrayExclus = listArray.filterNot(x => listeMotifExclus.exists(y => x.contains(y)))
        if (listeMotifOnly.isEmpty){
          listArrayExclus
        }
        else {
          val listArrayOnly = listArrayExclus.filter(x => listeMotifOnly.exists(y => x.contains(y)))
          listArrayOnly
        }
      }
    }
    else {
        val newListStatus = listStatus.filterNot(x => listeCheminsExclus.exists(y => y.matches(x.getPath.toString.split(":").last)))
        val listeReturnFile = newListStatus.map(_.getPath).filter(fs.isFile(_)).map(_.toString)
        val ListeReturnFileExclus = listeReturnFile.filterNot(x => listeMotifExclus.exists(y => x.contains(y)))
      if (listeMotifOnly.isEmpty){
        ListeReturnFileExclus
      }
      else {
        val listArrayOnly = ListeReturnFileExclus.filter(x => listeMotifOnly.exists(y => x.contains(y)))
        listArrayOnly
      }
    }
  }
  def listerRepertoireHdfs(spark:SparkSession,chemin:String):Unit={
    import org.apache.hadoop.fs.{FileSystem, Path}

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val status = fs.listStatus(new Path(chemin))
    status.foreach(x=> println(x.getPath))}
}
