# Multi-stage build for OpenDT Digital Twin System
FROM python:3.10-slim AS base

# Install system dependencies including Java 21 (standard for Debian Trixie)
RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy Python requirements and install dependencies including lz4
COPY requirements.txt* ./
RUN pip install --no-cache-dir \
    pandas \
    kafka-python \
    langchain-openai \
    pydantic \
    pyarrow \
    lz4

# Copy the entire project
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-arm64

# Expose any ports if needed (optional)
EXPOSE 9092

# Run the main digital twin system
CMD ["python", "opendt-orchestrator/digital_twin_runner.py"]
