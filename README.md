# Spark HDFS Utils

**Spark HDFS Utils** est un ensemble de classes et de fonctions facilitant l’interaction entre **Apache Spark** et un cluster **Hadoop HDFS**. Il propose notamment des méthodes de traitement de données, des utilitaires pour manipuler des DataFrames et des fonctionnalités pour gérer des chemins et répertoires sous HDFS.

---

## Fonctionnalités

Le projet est organisé autour de trois grands **packages** :

1. **Recette** (fonctions de transformation et de traitement de données)
   - **fonctionsRecette.InfoFiles**
     - `addNameColumn`
     - `creerIndexDataFrame`
     - `supprimerLignesDataFrame`
     - `getInfoFiles`, `printInfoFiles`
     - `getSizeFiles`, `printSizeFiles`
     - `getRawCountFiles`, `printRawCountFiles`
     - `getColumnFiles`, `printColumnFiles`
     - `writeInfoFiles`

2. **Utils** (fonctions utilitaires pour Spark et fichiers)
   - **fonctionsUtils.UtilsSpark**
     - `creerSparkSession`
     - `creerEtSetBasePardefaut`
   - **fonctionsUtils.UtilsFile**
     - `writeSingleFile`
     - `readFile`
     - `controleColonnesDf`
     - `countLines`
   - **fonctionsUtils.UtilsDev**
     - `compareRegex`
     - `biggestSizeFile`
     - `mostRecentDateFile`
     - `compareDates`

3. **Hdfs** (fonctions utilitaires pour gérer HDFS)
   - **fonctionsHdfs.UtilsHdfs**
     - `controlExistenceChemin`
     - `getDirsWithFiles`
     - `listerRepertoireHdfs`

Ces méthodes couvrent différents besoins du développement Spark : création de **Session Spark**, manipulation de **DataFrames**, interactions avec **HDFS**, et opérations variées comme la comparaison de regex, la lecture/écriture de fichiers, etc.

---

## Tester le package et modifier la classe d’entrée

Afin de **tester le package**, vous pouvez modifier le fichier
`app/src/main/scala/runFonctions/EssaiMain.scala`
et lancer la commande :
```bash
sbt run
```
dans le **dossier racine du projet**.

Si vous souhaitez **changer la classe d’entrée par défaut**, éditez le fichier `build.sbt` et modifiez la ligne :
```scala
mainClass in (Compile, run) := Some("runFonctions.EssaiMain")
```

---

## Utilisation du Toolkit Docker

Le projet propose un **toolkit** permettant de lancer le code Scala/Spark sans installer localement Java, Scala, SBT ni Spark.

> **Note** : Vous aurez besoin d’un compte Docker pour accéder au Hub Docker et, si nécessaire, pousser vos images. Les identifiants sont configurés dans le fichier `.env` (non versionné par git), au sein du dossier `toolkit`.

### Configuration

1. Dans le dossier `toolkit/`, créez un fichier `.env` similaire à :
   ```
    DOCKER_USER=xxxxxxxx
    DOCKER_PASS=xxxxxxxxxxxx
    JAVA_VERSION=11.0.14.1
    SBT_VERSION=1.6.2
    SCALA_VERSION=2.12.15
   ```
   - Ajustez les versions par défaut de Java, Scala et SBT si besoin.

2. Rendez le script `sbt.sh` exécutable :
   ```bash
   chmod +x sbt.sh
   ```

### Commandes disponibles

Le script `sbt.sh` propose quatre commandes :

- `bash` : ouvre un terminal bash dans le conteneur
- `run` : exécute `sbt run` (lance la classe main)
- `package` : exécute `sbt package` (compile et génère le .jar)
- `test` : exécute les tests unitaires

#### Exemple d’utilisation

```bash
cd toolkit

./sbt.sh help     # Affiche l'aide
# -->  Usage: ./sbt.sh {bash|run|package|test} [JAVA_VERSION=xxx] [SBT_VERSION=xxx] [SCALA_VERSION=xxx]
./sbt.sh run      # Lance le code Scala
./sbt.sh bash     # Ouvre un terminal bash dans le conteneur
./sbt.sh package  # Compile le projet
./sbt.sh test     # Lance les tests unitaires
```

---

## Tests Unitaires

Si des tests unitaires sont implémentés, vous pouvez les lancer via Docker en utilisant le toolkit :

```bash
cd toolkit
./sbt.sh test
```

---

# Demo

![video demo](docs/demo.webm)

---

## Références

- **Hadoop**
  - [bigdatafoundation/docker-hadoop](https://github.com/bigdatafoundation/docker-hadoop)
  - [Article Medium sur la configuration d’un cluster HDFS Docker](https://bytemedirk.medium.com/setting-up-an-hdfs-cluster-with-docker-compose-a-step-by-step-guide-4541cd15b168)

- **Spark**
  - [Image Docker Bitnami Spark](https://hub.docker.com/r/bitnami/spark/tags)
  - [Github Bitnami Spark](https://github.com/bitnami/containers/tree/main/bitnami/spark)

- **Scala et SBT**
  - [Docker SBT](https://hub.docker.com/r/sbtscala/scala-sbt)
  - [GitHub Docker SBT](https://github.com/sbt/docker-sbt)

- **Maven Repository**
  - [mvnrepository.com](https://mvnrepository.com/)

