FROM quay.io/astronomer/astro-runtime:13.2.0

# Install system packages
USER root
COPY packages.txt .
RUN apt-get update && \
    cat packages.txt | xargs apt-get install -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configure Git for DVC operations
RUN git config --global user.email "airflow@mlops.local" && \
    git config --global user.name "Airflow MLOps Pipeline" && \
    git config --global init.defaultBranch main

# Switch back to astro user
USER astro