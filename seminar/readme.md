# Доп занятие по spark

## Настройка

### 
### Kafka
1. Создаем Object storage (сохраняем ключи для доступа RW)
2. Создаем ВМ (4 core, 16 Gb RAM, 50Gb HDD)
3. Со своей машины выполняем
    ```shell
    scp -r ../practice5/docker/ apilipenko@51.250.48.237:~/
    scp -r ../practice5/data_producer.py apilipenko@51.250.48.237:~/
    scp -r ../practice5/streaming_job.py apilipenko@51.250.48.237:~/
    scp -r ../requirements.txt  apilipenko@51.250.48.237:~/
    ```
4. На ВМ выполняем
   ```shell 
   sudo apt install -y docker-compose
   sudo groupadd docker
   sudo usermod -aG docker $USER
   ```
   Logout
   ```shell
   sudo apt install -y python3-pip
   pip install -r requirements.txt
   sudo apt install -y awscli
   aws s3 ls --human-readable s3://nyc-tlc/trip\ data/ --no-sign-request
   ```
   Выбираем понравившийся файл
5.
 ```shell
 aws s3 cp s3://nyc-tlc/trip\ data/yellow_tripdata_2019-06.csv --no-sign-request ./
 aws configure # вводим ключи и регион ru-central1
 aws s3 --endpoint-url=https://storage.yandexcloud.net ls s3://seminar-data

 ```
6. Запускаем kafka

```shell
cd docker/
docker-compose up -d
docker stats
```

6. Запускаем генерацию данных
    ```shell
    python3 data_producer.py
    ```
    1. Сздаем ВМ airflow
        1. `scp -r docker  apilipenko@178.154.244.191:~/`
        2. ```shell
         sudo apt install -y docker
         sudo groupadd docker
         sudo usermod -aG docker $USER
         sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
         sudo chmod +x /usr/local/bin/docker-compose
         ```
        3. ```shell
         cd docker/airflow/
         
         ```
7. Создаем кластер Dataproc (версия образа 2.0+)
    1. На `MasterNode` необходимо добавить публичный адрес
    2. Копируем папку `scripts` на `MasterNode`:  ` scp -r scripts ubuntu@<ip>:~/`
    3. Если `streaming.sh` не имеет execute - `chmod +x streaming.sh`
    4. Подключаемся к `MasterNode`:  `ssh -A ubuntu@<ip>`
    5. `cd ~/scripts`
    6. Качаем jar и подкладываем его (надо произвести на всех нодах при cluster deploy):
        1. `wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.6.2/commons-pool2-2.6.2.jar`
        2. `sudo cp commons-pool2-2.6.2.jar $SPARK_HOME/jars/`
    7. Качаем [Jmx exporter](https://github.com/prometheus/jmx_exporter)
        1. `wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.16.1/jmx_prometheus_javaagent-0.16.1.jar`
    8. Качаем [config file](https://raw.githubusercontent.com/prometheus/jmx_exporter/master/example_configs/spark.yml)
        1. `wget https://raw.githubusercontent.com/prometheus/jmx_exporter/master/example_configs/spark.yml`
    9. `./streaming.sh`
    10.
8. 
