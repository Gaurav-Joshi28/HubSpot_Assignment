# =============================================================================
# Dockerfile for dbt-snowflake
# =============================================================================
# PURPOSE:
#   Build a Docker image with dbt-snowflake installed for running
#   the rental property analytics pipeline.
#
# USAGE:
#   docker build -t dbt-rental-property .
#   docker run -v $(pwd):/usr/app dbt-rental-property dbt run
# =============================================================================

FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR=/usr/app

# Set working directory
WORKDIR /usr/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ssh \
    && rm -rf /var/lib/apt/lists/*

# Install dbt-snowflake
RUN pip install --no-cache-dir \
    dbt-snowflake==1.10.2 \
    && pip cache purge

# Copy project files
COPY . /usr/app/

# Default command
CMD ["dbt", "run"]
