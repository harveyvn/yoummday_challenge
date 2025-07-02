FROM bitnami/spark:3.5.1


COPY log4j2.properties /opt/bitnami/spark/conf/log4j2.properties
COPY app/ /opt/bitnami/spark/app/

RUN pip install --no-cache-dir -r /opt/bitnami/spark/app/requirements.txt

WORKDIR /opt/bitnami/spark/app
