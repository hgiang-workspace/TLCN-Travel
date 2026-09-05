FROM python:3.11-slim

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    openssh-client \
    libpng-dev \
    libjpeg-dev \
    libfreetype6-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir airflow==2.10.2

COPY --chown=1000:1000 airflow/requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_UID=50000

WORKDIR /opt/airflow

COPY --chown=1000:1000 airflow/dags /opt/airflow/dags

EXPOSE 8080

HEALTHCHECK CMD /opt/airflow/scripts/healthcheck.sh || exit 1

ENTRYPOINT ["airflow", "webserver"]