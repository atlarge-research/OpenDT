FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy root pyproject.toml and install all dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

# Install shared library
COPY libs/common ./libs/common
RUN pip install --no-cache-dir -e ./libs/common

# Copy service code (overridden by volume mounts in development)
COPY services/sim-worker ./services/sim-worker

# Set Python path
ENV PYTHONPATH=/app/services/sim-worker:/app/libs/common:$PYTHONPATH

# Create non-root user
RUN useradd -m -u 1000 opendt && \
    chown -R opendt:opendt /app

USER opendt

CMD ["python", "-m", "sim_worker.main"]
