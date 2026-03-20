# Set the base image 
FROM python:3.11-slim-bullseye

# Adds metadata to image.
LABEL maintainer="Laurent"

# Évite la génération de fichiers .pyc et assure un affichage direct des logs (buffer)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Installation des dépendances système
# - gcc et libc-dev sont nécessaires pour compiler confluent-kafka si la wheel n'est pas dispo
# - librdkafka-dev est la bibliothèque de base pour Kafka
RUN apt-get update && apt-get install -y \
    curl \
    bash \
    openjdk-11-jre-headless \
    netcat \
    gcc \
    libc-dev \
    librdkafka-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Make a directory for our app
WORKDIR /producer/app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir pip --upgrade && \
    pip install --no-cache-dir -r requirements.txt
#RUN pip install --no-cache-dir confluent-kafka
#RUN pip install --no-cache-dir kafka-python-ng
#RUN pip install --no-cache-dir jupyter
#RUN pip install --no-cache-dir notebook
#RUN pip install --no-cache-dir pyspark

# Copy source code
COPY ./app .

EXPOSE 8000

# Run the application
CMD ["bash"]
# CMD ["python", "app/producer.py","velib-stations"]

# CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
