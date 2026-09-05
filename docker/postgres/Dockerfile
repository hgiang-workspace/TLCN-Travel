FROM postgres:15

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-contrib \
    && rm -rf /var/lib/apt/lists/*

ENV POSTGRES_USER=tourism
ENV POSTGRES_PASSWORD=tourism
ENV POSTGRES_DB=tourism

EXPOSE 5432

HEALTHCHECK CMD pg_isready -U tourism -d tourism || exit 1

CMD ["postgres"]