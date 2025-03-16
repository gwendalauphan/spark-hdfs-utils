package runFonctions

import fonctionsRecette.InfoFiles
import fonctionsUtils.UtilsSpark
import fonctionsUtils.UtilsFile
import fonctionsUtils.UtilsDev
import fonctionsHdfs.UtilsHdfs

import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger

object DemoMain {

  val logger = Logger.getLogger(this.getClass.getName)

  /**
   * Liste les méthodes déclarées directement dans l'objet (en filtrant
   * l'héritage de AnyRef / Object) et les méthodes "spéciales" Scala
   * qui contiennent "$".
   */
  def listMethods(obj: AnyRef, label: String): Unit = {
    println(s"=== Méthodes de $label ===")
    // obj.getClass => par ex. fonctionsRecette.InfoFiles$ pour l'objet InfoFiles
    val methods = obj.getClass.getMethods
      // garde uniquement les méthodes dont la classe déclarante est la classe de l'objet
      .filter(m => m.getDeclaringClass == obj.getClass)
      // on ignore les méthodes internes Scala (souvent "$" dans le nom)
      .filterNot(m => m.getName.contains("$"))

    if (methods.isEmpty)
      println(s"Aucune méthode déclarée directement dans $label.")
    else {
      methods.foreach(m => println(s"- ${m.getName}"))
    }
    println()
  }

  def main(args: Array[String]): Unit = {

    // Appel de la fonction pour chacun des objets importés
    // (InfoFiles, UtilsSpark, UtilsFile, UtilsDev, UtilsHdfs).
    // Attention : on utilise "InfoFiles" (l'objet), pas "InfoFiles._"
    listMethods(InfoFiles,  "fonctionsRecette.InfoFiles")
    listMethods(UtilsSpark, "fonctionsUtils.UtilsSpark")
    listMethods(UtilsFile,  "fonctionsUtils.UtilsFile")
    listMethods(UtilsDev,   "fonctionsUtils.UtilsDev")
    listMethods(UtilsHdfs,  "fonctionsHdfs.UtilsHdfs")

    logger.info("Fin de la liste des méthodes.")
  }
}
