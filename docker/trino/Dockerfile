FROM trino/trino:420

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    sudo \
    && rm -rf /var/lib/apt/lists/*

COPY --chown=1000:1000 docker/trino/config.properties /etc/trino/config.properties
COPY --chown=1000:1000 docker/trino/jvm.config /etc/trino/jvm.config
COPY --chown=1000:1000 docker/trino/node.properties /etc/trino/node.properties
COPY --chown=1000:1000 docker/trino/catalog/iceberg.properties /etc/trino/catalog/iceberg.properties

EXPOSE 8080

HEALTHCHECK CMD curl -f http://localhost:8080/health || exit 1

CMD ["trino", "--server-name", "trino", "--node-properties-file", "/etc/trino/node.properties"]