# spark_hdfs_utils

**Fonctions recette**
-

**3 Packages principaux:**

    - FonctionsHdfs:
        - Fonctions dédiées à l'utilisation hadoop hdfs sous spark

    - FonctionsUtils:
        - Dev: Fonctions utiles au développement de projets scala
        - File: Fonctions utiles au développement liés aux fichiers sous spark
        - Spark: Fonctions utiles générales pour spark

    - FonctionsRecette:
        -InfoFiles: Fonctions utiles au développement de recette sous spark scala



Le projet est lié à gitlab-runner, doc à venir ...


**Commandes Gitlab-runner/Docker**
-

Lien d'aide: https://datawookie.dev/blog/2021/03/install-gitlab-runner-with-docker/

2 manières de mettre en place un runner:

    - via l'installation de gitlab-runner sur la machine affectée
    - via la mise en place d'un conteneur gitlab-runner

**Mise en place via gitlab-runner:**



**Mise en place via un conteneur (docker):**

1.  `sudo docker run -d --name gitlab-runner --restart always   -v /srv/gitlab-runner/config:/etc/gitlab-runner   -v /var/run/docker.sock:/var/run/docker.sock   gitlab/gitlab-runner:latest`

2. `sudo docker exec -it gitlab-runner gitlab-runner register --docker-privileged`

Entrer les infos suivantes:

    - adresse url du gitlab
    - token
    - description du runner
    - tags (optionnel, peut laisser blanc)
    - l'executeur (ssh, shell, docker ...) #Dans ce projet, nous utilisons docker
    - image docker par défaut (si docker choisi) #On pourra aussi la spécifier dans le fichier gitlab-ci.yml


3. `sudo docker exec -it gitlab-runner gitlab-runner list`

4. `sudo docker exec -it gitlab-runner gitlab-runner start`


**Fichier de configuration**
-

Vous pouvez changer la configuration ou rajouter de nouveaux paramètres

5. `sudo vi /srv/gitlab-runner/config/config.toml`

6. Rajouter dans la partie [runners.docker]
    - `pull_policy = ["if-not-present"] `#Permet de charger des images disponibles seulement en local


**Commandes Git pour utilisation de la pipeline**
-

**Pour une pipeline normale de test:**

- git add .
- git commit -m "message"
- git push


**Pour une pipeline de mise à jour de branche:**

- git merge dev  #merge dev into manières


**Pour une pipeline de mise à jour de dépot nexus**

- git tag -a v1 -m "nexus"
- git push origin v1



-------------------------

!reference [.shared_hidden_key] #= .shared_hidden_key: &shared
!reference [.shared_set_no_proxy] #= .shared_set_no_proxy: &set_no_proxy

!reference [.cache-sbt] #= .cache-sbt: &cache-sbt
!reference [.cache-api-maacdo] #= .cache-api-maacdo: &cache-api-maacdo
!reference [.cache-ciValues] #= .cache-ciValues: &cache-ciValues


# Refs

- https://hub.docker.com/r/bitnami/spark/tags
- https://bytemedirk.medium.com/setting-up-an-hdfs-cluster-with-docker-compose-a-step-by-step-guide-4541cd15b168
- https://github.com/bigdatafoundation/docker-hadoop/blob/master/3.3.6/docker-compose.yml
