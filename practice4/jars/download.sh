mkdir ~/.mysql && \                                                                                                        [2]
wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O ~/.mysql/root.crt && \
chmod 0600 ~/.mysql/root.crt

wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.25/mysql-connector-java-8.0.25.jar