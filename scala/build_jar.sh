#!/bin/bash

source .env

docker login -u $DOCKER_USER -p $DOCKER_PASS

# Étape 1: Construire l'image Docker
docker build --build-arg JAVA_VERSION="graalvm-community-21.0.2" \
  --build-arg SBT_VERSION="1.10.10" \
  --build-arg SCALA_VERSION="3.3.5" \
  --build-arg USER_ID=1001 \
  --build-arg GROUP_ID=1001 \
  -t scala-build -f ./Dockerfile ../.

# Étape 2: Exécuter le conteneur pour compiler le JAR
docker run --rm -v $(pwd)/target:/app/target scala-build sbt package

# Le JAR sera disponible dans ./target/scala-2.12/
echo "Le fichier JAR est disponible dans target/scala-2.12/"
