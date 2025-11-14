"""Command-line entry points for OpenDT."""
from __future__ import annotations

from pathlib import Path
import sys

# When executed as a script (python src/opendt/cli.py) the package
# parent (`src`) is not on sys.path. Add it so absolute imports work.
if __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from .app import create_app
except Exception:
    # Fallback to absolute import for cases where package layout is importable
    from opendt.app import create_app


def main() -> None:
    app = create_app()
    app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)


if __name__ == "__main__":
    main()
