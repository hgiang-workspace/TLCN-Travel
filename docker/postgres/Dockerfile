FROM postgres:16

ENV POSTGRES_USER=tourism
ENV POSTGRES_PASSWORD=tourism
ENV POSTGRES_DB=tourism

EXPOSE 5432

HEALTHCHECK CMD pg_isready -U tourism -d tourism || exit 1

CMD ["postgres"]
