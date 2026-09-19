FROM python:3.11-slim

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    torch \
    transformers \
    datasets \
    tokenizers \
    scikit-learn \
    pandas \
    numpy \
    evaluate \
    accelerate \
    sentencepiece

COPY nlp/requirements.txt /opt/nlp/requirements.txt
RUN pip install --no-cache-dir -r /opt/nlp/requirements.txt

WORKDIR /opt/nlp

COPY nlp /opt/nlp

ENV PYTHONPATH=/opt/nlp

EXPOSE 5000

HEALTHCHECK CMD python -c "import torch; print(torch.__version__)" || exit 1

ENTRYPOINT ["python"]
