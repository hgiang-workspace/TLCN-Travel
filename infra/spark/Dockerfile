FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root

ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.12.0/iceberg-spark-runtime-3.5_2.12-1.12.0.jar /tmp/iceberg-spark-runtime.jar
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar /tmp/iceberg-aws-bundle.jar
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar /tmp/postgresql.jar

RUN echo "8dd8c90e54ed1a20f2461309d047a255ee56382eb7c0a46162b27a4936fd86dc  /tmp/iceberg-spark-runtime.jar" | sha256sum -c - \
    && echo "dd7e49ffa4ac9b215bed846581991259e3aec9bb2fa08d6cec029c46cd66d957  /tmp/iceberg-aws-bundle.jar" | sha256sum -c - \
    && echo "73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c  /tmp/postgresql.jar" | sha256sum -c - \
    && mv /tmp/iceberg-spark-runtime.jar /opt/spark/jars/ \
    && mv /tmp/iceberg-aws-bundle.jar /opt/spark/jars/ \
    && mv /tmp/postgresql.jar /opt/spark/jars/ \
    && chmod 644 /opt/spark/jars/iceberg-spark-runtime.jar \
    && chmod 644 /opt/spark/jars/iceberg-aws-bundle.jar \
    && chmod 644 /opt/spark/jars/postgresql.jar

# Keep the Spark runtime aligned with the project lockfile for the read-only
# inventory and other Python-side lakehouse utilities.
RUN python3 -m pip install --no-cache-dir \
    "PyYAML==6.0.2" \
    "minio==7.2.15" \
    "sentence-transformers==3.4.1" \
    "torch==2.12.0" \
    "transformers==4.41.2"

RUN python3 -m pip install --no-cache-dir "underthesea==9.5.0"

COPY scripts/smoke/spark_iceberg_smoke.py /opt/g01/spark_iceberg_smoke.py

USER 185
