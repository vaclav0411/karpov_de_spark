FROM apache/airflow:2.2.0-python3.8

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

USER root
RUN sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29
RUN apt update && apt install -y wget telnet && wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O /tmp/YandexInternalRootCA.crt

USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install -r /opt/airflow/requirements.txt
