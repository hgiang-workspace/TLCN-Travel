FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip curl \
    && rm -rf /var/lib/apt/lists/*

# Download standalone cluster jars
RUN curl -sL "https://repo1.maven.org/maven2/org/apache/spark/spark-worker_2.12/3.5.1/spark-worker_2.12-3.5.1.jar" -o "/opt/spark/jars/spark-worker_2.12-3.5.1.jar" && \
    ls /opt/spark/jars/ | grep worker

COPY spark/requirements.txt /opt/spark/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/spark/requirements.txt

# ViSoBERT dependencies
RUN pip3 install --no-cache-dir \
    transformers==4.44.2 \
    torch==2.3.0+ccpu --index-url https://download.pytorch.org/whl/cpu \
    underthesea==6.8.4 \
    sentencepiece==0.1.91 \
    tiktoken==0.7.0

ENV HF_HOME=/opt/models/hf
ENV TRANSFORMERS_OFFLINE=1
ENV HF_HUB_OFFLINE=1

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

WORKDIR /opt/spark

ENV SPARK_MODE=worker
ENV SPARK_MASTER_URL=spark://spark-master:7077

CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]
