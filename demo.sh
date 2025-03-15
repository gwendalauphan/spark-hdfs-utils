#!/bin/bash

set -eo pipefail

packageName="fonctions_recette"
classMainName="runFonctions.EssaiMain"
scalaLongVersion="2.12.15"
scalaShortVersion="2.12"
jarNamePrefix="${packageName}_${scalaShortVersion}"

dir_path=$(dirname $(realpath $0))
jarDirPath="$dir_path/scala/target/scala-$scalaShortVersion"

jarFileName=$(ls $jarDirPath | grep $jarNamePrefix)
jarFilepath="$jarDirPath/$jarFileName"

# Construire le JAR
./scala/sbt.sh package

# Copier le JAR dans le conteneur hadoop-namenode
docker cp $jarFilepath hadoop-namenode:/opt/hadoop/data/nameNode/$jarFileName

# Créer le répertoire /user/spark/apps et copier le JAR dans le conteneur spark-master
docker exec -it hadoop-namenode bash -c "hdfs dfs -mkdir -p /user/spark/apps"
docker exec -it hadoop-namenode bash -c "hdfs dfs -put -f /opt/hadoop/data/nameNode/$jarFileName /user/spark/apps"

# Exécuter le JAR
docker exec -it spark-master bash -c "spark-submit --master spark://spark-master:7077 --class ${classMainName}  hdfs://hadoop-namenode:9000/user/spark/apps/${jarFileName}"



