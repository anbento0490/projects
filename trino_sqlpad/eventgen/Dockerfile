
FROM python:3.9-slim

# Install required system dependencies for psycopg2 and PostgreSQL client libraries
RUN apt-get update && apt-get install -y \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies including psycopg2-binary
RUN pip install \
    psycopg2-binary==2.9.3 \
    faker==13.3.2 \
    confluent-kafka

# Default command to keep container alive
CMD ["tail", "-F", "anything"]
