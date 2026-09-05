FROM bitnami/spark:3.5

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin:$JAVA_HOME/bin

COPY --chown=1000:1000 spark/requirements.txt /opt/spark/requirements.txt
RUN pip install --no-cache-dir -r /opt/spark/requirements.txt

WORKDIR /opt/spark

COPY --chown=1000:1000 spark/jobs /opt/spark/jobs

ENV SPARK_MODE=master
CMD ["spark-class", "org.apache.spark.deploy.master.SparkMaster", "--host", "spark-master", "--port", "7077"]