# RMS/notifications/waha_sender.py
import requests
import logging
import json

logger = logging.getLogger(__name__)

class WahaSender:
    """
    A class to handle sending messages via a WAHA (WhatsApp HTTP API) server.
    """
    def __init__(self, base_url: str, session_name: str):
        if not base_url or not session_name:
            raise ValueError("WAHA base_url and session_name are required.")
        
        self.base_url = base_url.rstrip('/')
        self.session_name = session_name
        self.headers = {"Content-Type": "application/json"}
        
        logger.info(f"WahaSender initialized for session '{session_name}' at {self.base_url}")

    def send_message_to_group(self, group_id: str, message_text: str) -> bool:
        """
        Sends a text message to a specific WhatsApp group.
        """
        if not group_id or not message_text:
            logger.warning("WAHA_SENDER: Group ID or message text is empty. Cannot send message.")
            return False

        endpoint = f"/api/sendText"
        url = f"{self.base_url}{endpoint}"
        
        payload = {
            "chatId": group_id,
            "text": message_text,
            "session": self.session_name
        }
        
        response = None
        try:
            logger.info(f"WAHA_SENDER: Sending message to group {group_id} via session {self.session_name}.")
            logger.debug(f"WAHA_SENDER: Payload: {json.dumps(payload)}")
            
            response = requests.post(url, headers=self.headers, json=payload, timeout=15)
            response.raise_for_status()
            
            response_json = response.json()
            logger.info(f"WAHA_SENDER: Message sent successfully. Response: {response_json}")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"WAHA_SENDER: Failed to send message. Error: {e}", exc_info=True)
            if response is not None:
                logger.error(f"WAHA_SENDER: Response Status: {response.status_code}, Response Text: {response.text}")
            return False
        except json.JSONDecodeError as e:
            logger.error(f"WAHA_SENDER: Failed to decode JSON response. Error: {e}", exc_info=True)
            if response is not None:
                logger.error(f"WAHA_SENDER: Raw Response Text: {response.text}")
            return False

    def check_session_status(self) -> str:
        """
        Checks the status of the configured WAHA session.
        Returns the status string (e.g., "WORKING", "SCAN_QR_CODE") or "ERROR".
        """
        # --- THIS IS THE FIX ---
        # The correct endpoint for checking a session is /api/sessions/{session_name}
        endpoint = f"/api/sessions/{self.session_name}"
        # --- END FIX ---
        
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Based on your reference code, a working session has status 'WORKING'
            # and the engine state is 'CONNECTED'. Let's check both for robustness.
            is_working = (
                data.get('status') == 'WORKING' and 
                data.get('engine', {}).get('state') == 'CONNECTED'
            )
            
            status = "WORKING" if is_working else data.get('status', 'UNKNOWN')
            
            logger.info(f"WAHA_SENDER: Session '{self.session_name}' status is '{status}'. Full response: {data}")
            return status
        except requests.exceptions.RequestException as e:
            logger.error(f"WAHA_SENDER: Could not check session status. Error: {e}", exc_info=True)
            return "ERROR"