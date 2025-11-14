"""Application factory for the OpenDT service."""
from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from flask import Flask

# Load environment variables during app startup (keeps package import clean)
load_dotenv()  # default discovery
load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

from .api.routes import api_bp, ui_bp
from .logging_config import configure_logging  # noqa: F401  # ensures logging is configured


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
