#!/bin/bash

set -eo pipefail

if [ "$1" == "help" ]; then
  echo "Usage: $0 {bash|run|package|test} [JAVA_VERSION=xxx] [SBT_VERSION=xxx] [SCALA_VERSION=xxx]"
  exit 1
elif [ "$1" != "bash" ] && [ "$1" != "run" ] && [ "$1" != "package" ] && [ "$1" != "test" ]; then
  echo "Usage: $0 {bash|run|package|test} [JAVA_VERSION=xxx] [SBT_VERSION=xxx] [SCALA_VERSION=xxx]"
  exit 1
fi

COMMAND=$1
shift

dir_path=$(dirname $(realpath $0))
project_path=$(dirname $dir_path)

source $dir_path/.env

docker login -u $DOCKER_USER -p $DOCKER_PASS

# Valeurs par défaut sont définies dans le fichier .env
# Override with input parameters if provided
for arg in "$@"; do
  case $arg in
    JAVA_VERSION=*)
      JAVA_VERSION="${arg#*=}"
      shift
      ;;
    SBT_VERSION=*)
      SBT_VERSION="${arg#*=}"
      shift
      ;;
    SCALA_VERSION=*)
      SCALA_VERSION="${arg#*=}"
      shift
      ;;
    *)
      echo "Usage: $0 {bash|run|package|test} [JAVA_VERSION=xxx] [SBT_VERSION=xxx] [SCALA_VERSION=xxx]"
      exit 1
      ;;
  esac
done


SCALA_SHOT_VERSION=$(echo $SCALA_VERSION | cut -d. -f1,2)

# Affichage de la configuration
echo "Configuration:"
echo "Commande: $COMMAND"
echo "JAVA_VERSION=$JAVA_VERSION"
echo "SBT_VERSION=$SBT_VERSION"
echo "SCALA_VERSION=$SCALA_VERSION"
echo "SCALA_SHOT_VERSION=$SCALA_SHOT_VERSION"
echo


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
if [ "$COMMAND" == "bash" ]; then
  docker run --rm -it -v $dir_path/target:/app/target scala-build bash
elif [ "$COMMAND" == "run" ]; then
  docker run --rm -v $dir_path/target:/app/target scala-build sbt run
elif [ "$COMMAND" == "package" ]; then
  docker run --rm -v $dir_path/target:/app/target scala-build sbt package
elif [ "$COMMAND" == "test" ]; then
  docker run --rm -v $dir_path/target:/app/target scala-build sbt test
else
  echo "$1"
  echo "Usage: $0 {bash|run|package|test} [JAVA_VERSION=xxx] [SBT_VERSION=xxx] [SCALA_VERSION=xxx]"
  exit 1
fi

# Le JAR sera disponible dans ./target/scala-2.12/ si l'option package est utilisée
if [ "$1" == "package" ]; then
  echo "Le fichier JAR est disponible dans $dir_path/target/scala-$SCALA_SHOT_VERSION/"
fi

