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

# Ensure Python can resolve the in-repo package layout
if [ -d /app/src ]; then
  case ":$PYTHONPATH:" in
    *":/app/src:"*) ;;
    *) export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}/app/src" ;;
  esac
fi

# Ensure the OpenDC runner is executable (covers bind-mount cases)
OPENDC_ROOT="/app/src/opendt/core/simulation/opendc"
RUNNER="${OPENDC_ROOT}/bin/OpenDCExperimentRunner/bin/OpenDCExperimentRunner"

if [ ! -f "$RUNNER" ]; then
  RUNNER="${OPENDC_ROOT}/bin/OpenDCExperimentRunner/OpenDCExperimentRunner"
fi

if [ ! -f "$RUNNER" ]; then
  # Legacy fallback for older volume layouts
  LEGACY_ROOT="/app/opendt-simulator/bin/OpenDCExperimentRunner"
  if [ -f "${LEGACY_ROOT}/bin/OpenDCExperimentRunner" ]; then
    RUNNER="${LEGACY_ROOT}/bin/OpenDCExperimentRunner"
    OPENDC_ROOT="${LEGACY_ROOT}"
  elif [ -f "${LEGACY_ROOT}/OpenDCExperimentRunner" ]; then
    RUNNER="${LEGACY_ROOT}/OpenDCExperimentRunner"
    OPENDC_ROOT="${LEGACY_ROOT}"
  fi
fi

if [ -f "$RUNNER" ]; then
  chmod +x "$RUNNER" || true
  # Make sure its directories are traversable
  chmod -R a+rx "$OPENDC_ROOT" || true
fi

#give perms to run script
#chmod 777 /app/run.sh

exec "$@"
