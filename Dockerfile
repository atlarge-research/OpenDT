FROM python:3.10-slim

# Install Java JDK 21 and essentials
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jdk-headless \
    ca-certificates wget unzip \
 && rm -rf /var/lib/apt/lists/*

# Derive JAVA_HOME from the installed 'java', and provide stable symlinks
RUN set -eux; \
    JH="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"; \
    mkdir -p /usr/lib/jvm; \
    ln -sfn "$JH" /usr/lib/jvm/default-java; \
    # Ensure compatibility with scripts expecting this exact path
    if [ ! -e /usr/lib/jvm/java-21-openjdk-amd64 ]; then \
      ln -s "$JH" /usr/lib/jvm/java-21-openjdk-amd64; \
    fi

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

# Ensure the package layout under ./src is importable at runtime
ENV PYTHONPATH=/app/src

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY . .

# Best-effort: if baked in, set runner executable
RUN if [ -f /app/opendt-simulator/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner ]; then \
      chmod +x /app/opendt-simulator/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner; \
    fi

# Entrypoint ensures runtime perms and JAVA_HOME PATH before launching
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python", "-m", "opendt.cli"]