#!/bin/bash

set -eo pipefail

dir_path=$(dirname $(realpath $0))
project_path=$(dirname $dir_path)

source $dir_path/.env

docker login -u $DOCKER_USER -p $DOCKER_PASS

JAVA_VERSION="11.0.14.1"
SBT_VERSION="1.6.2"
SCALA_VERSION="2.12.15"

# Étape 1: Construire l'image Docker
docker build --build-arg JAVA_VERSION=$JAVA_VERSION \
  --build-arg SBT_VERSION=$SBT_VERSION \
  --build-arg SCALA_VERSION=$SCALA_VERSION \
  --build-arg USER_ID=1001 \
  --build-arg GROUP_ID=1001 \
  -t scala-build -f $dir_path/Dockerfile $dir_path/..

# Supprimer les anciennes images scala-build sans tag
docker images -f "dangling=true" -q | xargs -r docker rmi

# Étape 2: Vérifier les arguments et exécuter le conteneur en conséquence
if [ "$1" == "bash" ]; then
  docker run --rm -it -v $dir_path/target:/app/target scala-build bash
elif [ "$1" == "run" ]; then
  docker run --rm -v $dir_path/target:/app/target scala-build sbt run
elif [ "$1" == "package" ]; then
  docker run --rm -v $dir_path/target:/app/target scala-build sbt package
elif [ "$1" == "test" ]; then
  docker run --rm -v $dir_path/target:/app/target scala-build sbt test
else
  echo "Usage: $0 {bash|run|package|test}"
  exit 1
fi

# Le JAR sera disponible dans ./target/scala-2.12/ si l'option package est utilisée
if [ "$1" == "package" ]; then
  echo "Le fichier JAR est disponible dans $dir_path/target/scala-2.12/"
fi

