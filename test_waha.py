# test_waha.py
from notifications.waha_sender import WahaSender
import logging

# Basic logging setup for the test
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURE THESE ---
WAHA_URL = "http://129.159.18.115:49165"
WAHA_SESSION = "us-phone-bot"
GROUP_ID = "120363418523579709@g.us"
# --- END CONFIGURATION ---

if __name__ == "__main__":
    print("Initializing WAHA Sender...")
    sender = WahaSender(base_url=WAHA_URL, session_name=WAHA_SESSION)
    
    print("\nChecking session status...")
    status = sender.check_session_status()
    print(f"Session status is: {status}")
    
    if status == "WORKING":
        print("\nSending test message to group...")
        test_message = "Hello from the RMS Notification Module! This is a test message."
        success = sender.send_message_to_group(group_id=GROUP_ID, message_text=test_message)
        
        if success:
            print("Test message sent successfully!")
        else:
            print("Failed to send test message. Check the logs.")
    else:
        print("Session is not working. Cannot send message.")