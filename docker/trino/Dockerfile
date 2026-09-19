FROM trinodb/trino:435

USER root

COPY docker/trino/config.properties /etc/trino/config.properties
COPY docker/trino/jvm.config /etc/trino/jvm.config
COPY docker/trino/node.properties /etc/trino/node.properties

EXPOSE 8080
