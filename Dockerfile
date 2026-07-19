FROM australia-southeast1-docker.pkg.dev/cpg-common/images/cpg_hail_gcloud:0.2.138.cpg1-1

ENV PYTHONDONTWRITEBYTECODE=1
ENV VERSION=0.4.2

RUN apt-get update && apt-get install -y \
        tabix && \
    rm -r /var/lib/apt/lists/* && \
    rm -r /var/cache/apt/*

WORKDIR /cpg_flow_stripy

COPY src src/
COPY LICENSE pyproject.toml README.md ./

# pip install but don't retain the cache files
RUN pip install --no-cache-dir .
