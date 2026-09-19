FROM minio/minio:latest

EXPOSE 9000 9001

ENV MINIO_BROWSER_REDIRECT_URL=http://localhost:9001

HEALTHCHECK CMD curl -f http://localhost:9000/minio/health/live || exit 1

ENTRYPOINT ["minio", "server", "/data", "--console-address", ":9001"]
