# Set python version
ARG BASE_CONTAINER=python:3.11-slim-bullseye

# Set the base image 
FROM python:3.11-slim-bullseye

# Adds metadata to image.
LABEL maintainer="Laurent"

# dont write pyc files
ENV PYTHONDONTWRITEBYTECODE 1
# dont buffer to stdout/stderr
ENV PYTHONUNBUFFERED 1

# Sets the user name to use when running the image.
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    bash \
    openjdk-11-jre-headless \
    netcat \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pandas pyspark

# Port exposé
EXPOSE 9999

# Make a directory for app
WORKDIR /consumer/app

# Install dependencies
COPY requirements.txt .
RUN  pip install --no-cache-dir -r requirements.txt
# RUN pip install --no-cache-dir --user -r /req.txt

# Copy source code
COPY ./app .

# Run the application
# CMD ["bash"]
# CMD ["python", "app/consumer.py"]
