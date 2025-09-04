# RMS/webhook_server.py
from flask import Flask, request, jsonify
import os
import sys
import logging

# Add the project root to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import your existing modules
from utils.config_loader import APP_CONFIG
from notifications.notification_engine import run_all_checks

# --- Setup Logging (same as run_notifications.py) ---
log_config = APP_CONFIG.get('logging', {})
log_file = log_config.get('file_name', 'rms_app.log')
log_level = getattr(logging, log_config.get('level', 'INFO').upper(), logging.INFO)
log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logging.basicConfig(
    level=log_level,
    format=log_format,
    handlers=[
        logging.FileHandler(os.path.join(project_root, log_file)),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Create the Flask App ---
app = Flask(__name__)

# --- Define the Webhook Endpoint ---
@app.route('/trigger-notifications', methods=['GET'])
def trigger_notifications():
    """
    A dedicated webhook endpoint to trigger the notification engine.
    URL: /trigger-notifications?key=YOUR_SECRET_KEY
    """
    logger.info("FLASK_WEBHOOK: Received a request.")
    
    # Get the secret key from the loaded APP_CONFIG (which now uses st.secrets under the hood)
    # For a non-Streamlit script, we need to read secrets differently.
    # Let's get it from the YAML for simplicity in this standalone script.
    # Or better, from environment variables. For now, let's assume it's in settings.yaml for consistency.
    
    # Let's modify config_loader to handle this. For now, let's hardcode a placeholder.
    # A better approach would be to load secrets from a file or env var.
    # Let's assume you've added the key to your settings.yaml for this script.
    
    # Let's simplify: We'll read the key from settings.yaml for this script
    # Add this to your settings.yaml:
    # webhooks:
    #   notification_trigger_key: "Tr1gg3r_Rms_N0tify_@2024!_aBcXyZ"
    
    expected_key = APP_CONFIG.get("webhooks", {}).get("notification_trigger_key")
    received_key = request.args.get('key')

    if not expected_key:
        logger.error("FLASK_WEBHOOK: Secret trigger key is not configured in settings.yaml!")
        return jsonify({"status": "error", "message": "Server configuration error"}), 500

    if not received_key or received_key != expected_key:
        logger.warning(f"FLASK_WEBHOOK: Invalid or missing trigger key received.")
        return jsonify({"status": "error", "message": "Unauthorized"}), 401

    logger.info("FLASK_WEBHOOK: Valid key received. Running notification checks...")
    
    try:
        # Run the main notification function
        run_all_checks()
        success_message = "Notification checks completed successfully."
        logger.info(f"FLASK_WEBHOOK: {success_message}")
        return jsonify({"status": "success", "message": success_message}), 200
    except Exception as e:
        error_message = f"Webhook notification run failed: {e}"
        logger.error(error_message, exc_info=True)
        return jsonify({"status": "error", "message": error_message}), 500

if __name__ == '__main__':
    # This server is meant to be run with a production-ready WSGI server like Gunicorn,
    # but for simplicity, we can run it with Flask's built-in server.
    # Host '0.0.0.0' makes it accessible from the network.
    # Port 49176 is chosen to be different from the Streamlit app's port.
    app.run(host='0.0.0.0', port=49176, debug=False)