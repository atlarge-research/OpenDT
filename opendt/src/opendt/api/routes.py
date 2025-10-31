#!/usr/bin/env python3
import logging
import threading
from copy import deepcopy
from flask import Blueprint, jsonify, render_template, request

logger = logging.getLogger(__name__)


def create_routes(orchestrator):
    bp = Blueprint('opendt', __name__)

    @bp.route('/')
    def dashboard():
        return render_template("index.html", state=orchestrator.state)

    @bp.route('/api/status')
    def api_status():
        return jsonify(orchestrator.state)

    @bp.route('/api/start', methods=['POST'])
    def api_start():
        if orchestrator.state['status'] in ['stopped', 'error']:
            threading.Thread(target=orchestrator.start_system, daemon=True).start()
            return jsonify({'message': 'System is starting...'})
        return jsonify({'message': f'System is {orchestrator.state["status"]}'})

    @bp.route('/api/stop', methods=['POST'])
    def api_stop():
        if orchestrator.state['status'] in ['running', 'starting']:
            orchestrator.stop_system()
            return jsonify({'message': 'System stopped'})
        return jsonify({'message': 'System already stopped'})

    @bp.route('/api/topology')
    def api_topology():
        return jsonify({
            'current_topology': orchestrator.state.get('current_topology'),
            'best_config': orchestrator.state.get('best_config'),
            'topology_updates': orchestrator.state.get('topology_updates', 0),
        })

    @bp.route('/api/accept_recommendation', methods=['POST'])
    def api_accept_recommendation():
        try:
            best_config = orchestrator.state.get('best_config')

            if not best_config or 'config' not in best_config:
                return jsonify({'error': 'No recommendation available'}), 400

            recommended_topology = best_config['config']

            success = orchestrator.update_topology_file(recommended_topology)

            if success:
                return jsonify({
                    'message': 'Topology updated successfully with LLM recommendation',
                    'topology_updates': orchestrator.state.get('topology_updates', 0),
                })
            else:
                return jsonify({'error': 'Failed to update topology file'}), 500

        except Exception as e:
            logger.error(f"Error accepting recommendation: {e}")
            return jsonify({'error': str(e)}), 500

    @bp.route('/api/reset_topology', methods=['POST'])
    def api_reset_topology():
        try:
            orchestrator.load_initial_topology()
            return jsonify({'message': 'Topology reset to initial configuration'})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    @bp.route('/api/set_slo', methods=['POST'])
    def set_slo():
        try:
            data = request.get_json()
            energy_target = float(data.get('energy_target'))
            runtime_target = float(data.get('runtime_target'))

            if energy_target <= 0 or runtime_target <= 0:
                return jsonify({'error': 'Invalid target values'}), 400

            orchestrator.slo_targets['energy_target'] = energy_target
            orchestrator.slo_targets['runtime_target'] = runtime_target

            return jsonify({
                'status': 'success',
                'energy_target': energy_target,
                'runtime_target': runtime_target,
            })
        except (ValueError, TypeError, KeyError) as e:
            return jsonify({'error': str(e)}), 400

    @bp.route("/api/sim/timeseries")
    def api_sim_timeseries():
        with orchestrator.open_dc_results_lock:
            cpu_usages = [res["cpu_utilization"] for res in orchestrator.open_dc_results]
            power_usages = [res["energy_kwh"] for res in orchestrator.open_dc_results]
            sim_results_timestamps = deepcopy(orchestrator.open_dc_results_timestamps)

        res = {
            "cpu_usages": cpu_usages,
            "power_usages": power_usages,
            "timestamps": sim_results_timestamps,
        }

        logger.info(f"Sending opendc results of all time: {res}")

        return jsonify(res)

    return bp
