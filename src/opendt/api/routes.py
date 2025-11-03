"""Flask blueprints exposing the OpenDT API and dashboard."""
from __future__ import annotations

import logging
import threading
from copy import deepcopy

from flask import Blueprint, jsonify, render_template, request
from pydantic import ValidationError

from .dependencies import get_orchestrator
from .schemas import SLORequest

logger = logging.getLogger(__name__)

api_bp = Blueprint("api", __name__, url_prefix="/api")
ui_bp = Blueprint("ui", __name__)


@ui_bp.route("/")
def dashboard():
    orchestrator = get_orchestrator()
    return render_template("index.html", state=orchestrator.state)


@api_bp.route("/set_slo", methods=["POST"])
@api_bp.route("/submit_slo", methods=["POST"])
def set_slo():
    orchestrator = get_orchestrator()
    try:
        data = request.get_json(force=True, silent=False) or {}
        slo_request = SLORequest.model_validate(data)
    except ValidationError as exc:
        return jsonify({'error': str(exc)}), 400

    requested = {
        'energy_target': float(slo_request.energy_target),
        'runtime_target': float(slo_request.runtime_target),
    }

    result = orchestrator.update_slo_file(requested)
    if result == "error":
        return jsonify({'error': 'Failed to update SLO file'}), 500

    status = 'success' if result == "applied" else 'unchanged'

    return jsonify({
        'status': status,
        'energy_target': orchestrator.slo_targets['energy_target'],
        'runtime_target': orchestrator.slo_targets['runtime_target'],
    })


@api_bp.route('/status')
def api_status():
    orchestrator = get_orchestrator()
    return jsonify(orchestrator.state)


@api_bp.route('/start', methods=['POST'])
def api_start():
    orchestrator = get_orchestrator()
    if orchestrator.state['status'] in ['stopped', 'error']:
        threading.Thread(target=orchestrator.start_system, daemon=True).start()
        return jsonify({'message': 'System is starting...'})
    return jsonify({'message': f'System is {orchestrator.state["status"]}'})


@api_bp.route('/stop', methods=['POST'])
def api_stop():
    orchestrator = get_orchestrator()
    if orchestrator.state['status'] in ['running', 'starting']:
        orchestrator.stop_system()
        return jsonify({'message': 'System stopped'})
    return jsonify({'message': 'System already stopped'})


@api_bp.route('/topology')
def api_topology():
    orchestrator = get_orchestrator()
    return jsonify({
        'current_topology': orchestrator.state.get('current_topology'),
        'best_config': orchestrator.state.get('best_config'),
        'topology_updates': orchestrator.state.get('topology_updates', 0),
    })


@api_bp.route('/accept_recommendation', methods=['POST'])
def api_accept_recommendation():
    orchestrator = get_orchestrator()
    try:
        payload = request.get_json(silent=True) or {}
        proposed = payload.get('topology') if isinstance(payload, dict) else None

        best_config = orchestrator.state.get('best_config') or {}

        if proposed is not None:
            recommended_topology = proposed
        else:
            if 'config' not in best_config:
                return jsonify({'error': 'No recommendation available'}), 400
            recommended_topology = best_config['config']

        success = orchestrator.update_topology_file(recommended_topology)

        if success:
            merged = deepcopy(best_config) if isinstance(best_config, dict) else {}
            merged['config'] = deepcopy(recommended_topology)
            orchestrator.state['best_config'] = merged
            return jsonify({
                'message': 'Topology updated successfully with LLM recommendation',
                'topology_updates': orchestrator.state.get('topology_updates', 0),
                'applied_config': recommended_topology,
            })
        return jsonify({'error': 'Failed to update topology file'}), 500

    except Exception as exc:  # pragma: no cover - defensive logging path
        logger.error("Error accepting recommendation: %s", exc)
        return jsonify({'error': str(exc)}), 500


@api_bp.route('/reset_topology', methods=['POST'])
def api_reset_topology():
    orchestrator = get_orchestrator()
    try:
        orchestrator.load_initial_topology()
        return jsonify({'message': 'Topology reset to initial configuration'})
    except Exception as exc:  # pragma: no cover - defensive logging path
        return jsonify({'error': str(exc)}), 500


@api_bp.route("/sim/timeseries")
def api_sim_timeseries():
    orchestrator = get_orchestrator()
    res = orchestrator.simulation_timeseries()
    logger.info("Sending opendc results of all time: %s", res)
    res['timestamps'] = deepcopy(res['timestamps'])
    return jsonify(res)
