FROM python:3.11-slim

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    libpq-dev \
    gcc \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libwayland-client0 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dagster \
    dagster-webserver \
    dagster-postgres \
    boto3 \
    psycopg2-binary \
    pyyaml \
    requests \
    httpx \
    pydantic \
    rapidfuzz \
    playwright \
    playwright-stealth

RUN mkdir -p /opt/dagster/home
COPY dagster_project/dagster.yaml /opt/dagster/home/dagster.yaml
RUN mkdir -p /usr/local/lib/python3.11/site-packages && \
    echo "import sys; sys.path.insert(0, '/opt/dagster/project')" > /usr/local/lib/python3.11/site-packages/sitecustomize.py

RUN useradd --create-home --shell /bin/bash dagster

USER dagster

ENV DAGSTER_HOME=/opt/dagster/home
ENV PYTHONPATH=/opt/dagster/project
WORKDIR /opt/dagster

EXPOSE 3000

HEALTHCHECK CMD curl -f http://localhost:3000/health || exit 1

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
