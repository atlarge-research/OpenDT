#!/usr/bin/env python3
import logging
from pathlib import Path
from flask import Flask

from .api.routes import create_routes
from .orchestrator.controller import OpenDTOrchestrator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_app():
    base_dir = Path(__file__).resolve().parent
    app = Flask(
        __name__,
        template_folder=str(base_dir.parent / 'templates'),
        static_folder=str(base_dir.parent / 'static'),
    )

    orchestrator = OpenDTOrchestrator()
    app.orchestrator = orchestrator

    routes = create_routes(orchestrator)
    app.register_blueprint(routes)

    return app


def main():
    app = create_app()
    app.run(host='0.0.0.0', port=8080, debug=False, threaded=True)


if __name__ == '__main__':
    main()
