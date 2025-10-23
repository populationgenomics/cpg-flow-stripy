FROM australia-southeast1-docker.pkg.dev/cpg-common/images/cpg_hail_gcloud:0.2.134.cpg2-1

ENV PYTHONDONTWRITEBYTECODE=1
ENV PROD_PIPES_CONFIG_COMMIT_HASH=af47d708f2cf97aa8a6ca14217cf2f9a9c7509fe

WORKDIR /cpg_flow_stripy

COPY src src/
COPY LICENSE pyproject.toml README.md ./

# pip install but don't retain the cache files
RUN pip install --no-cache-dir . && \
    git clone https://github.com/populationgenomics/production-pipelines-configuration.git && \
    cd production-pipelines-configuration && \
    git checkout $PROD_PIPES_CONFIG_COMMIT_HASH && \
    cd ..
