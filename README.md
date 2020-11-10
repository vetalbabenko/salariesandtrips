Pre-requirements:
-docker, docker-compose, java and scala should be installed on you computer and added to variable path.


1. Set up a kafka in docker
    - pull docker image: git clone https://github.com/wurstmeister/kafka-docker.git
    - cd cloned project and replace docker-compose.yml with one which located in docker-compose: docker-compose-kafka.yml
    - run next command: docker-compose up -d
    - verify if everything is running: docker-compose ps
    - you should observer 2 process running: kafka and zookeper


2. Set up a kafka in docker
    - pull docker image: git clone https://github.com/m-semnani/bd-infra.git
    - cd cloned project and replace docker-compose.yml with one which located in docker-compose: docker-compose-hadoop.yml
    - run next command: docker-compose up -d
    - verify if everything is running: docker-compose ps
    - you should observer 2 process running
 

4. To verify if data save to hdfs, you also need hadoop binaries:
    - download it from officia website and extract https://apache.ip-connect.vn.ua/hadoop/common/hadoop-2.10.1/hadoop-2.10.1.tar.gz 
    - use next commands to verify: bin/hdfs dfs -ls "hdfs://172.17.0.1:9092"
     