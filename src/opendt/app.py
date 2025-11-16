"""Application factory for the OpenDT service."""
from __future__ import annotations

from pathlib import Path

from flask import Flask

from .logging import configure_logging  # noqa: F401  # ensures logging is configured FIRST
from .api.routes import api_bp, ui_bp


def create_app() -> Flask:
    package_root = Path(__file__).resolve().parents[1]
    template_folder = package_root / "templates"
    static_folder = package_root / "static"

    app = Flask(
        __name__,
        template_folder=str(template_folder),
        static_folder=str(static_folder),
    )

    app.register_blueprint(api_bp)
    app.register_blueprint(ui_bp)
    return app
