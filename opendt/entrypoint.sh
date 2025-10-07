#!/bin/sh
set -e

# Recompute JAVA_HOME safely in case image/base changes
if command -v java >/dev/null 2>&1; then
  JH="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
  if [ -d "$JH" ]; then
    export JAVA_HOME="$JH"
    export PATH="$JAVA_HOME/bin:$PATH"
  fi
fi

# Ensure the OpenDC runner is executable (covers bind-mount cases)
RUNNER="/app/opendt-simulator/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner"
if [ -f "$RUNNER" ]; then
  chmod +x "$RUNNER" || true
  # Make sure its directories are traversable
  chmod -R a+rx "/app/opendt-simulator/bin/OpenDCExperimentRunner" || true
fi

#give perms to run script
chmod 777 ./run.sh

exec "$@"
