FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip curl \
    && rm -rf /var/lib/apt/lists/*

# Download standalone cluster jars
RUN curl -sL "https://repo1.maven.org/maven2/org/apache/spark/spark-master_2.12/3.5.1/spark-master_2.12-3.5.1.jar" -o "/opt/spark/jars/spark-master_2.12-3.5.1.jar" && \
    curl -sL "https://repo1.maven.org/maven2/org/apache/spark/spark-worker_2.12/3.5.1/spark-worker_2.12-3.5.1.jar" -o "/opt/spark/jars/spark-worker_2.12-3.5.1.jar" && \
    ls /opt/spark/jars/ | grep -E 'master|worker'

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

WORKDIR /opt/spark

ENV SPARK_MODE=master
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master", "--host", "spark-master", "--port", "7077"]
