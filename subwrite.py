import paho.mqtt.client as mqtt
import time
import os
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()

# --- MQTT Configuration (read from environment) ---
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC")
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

# --- Target Configuration (now also from environment) ---
TARGET_PREFIX = os.getenv("TARGET_PREFIX") # <-- READ FROM .env
OUTPUT_FILE = "matched_data.txt"

# Check if ALL required variables were loaded
if not all([MQTT_BROKER, MQTT_TOPIC, MQTT_USERNAME, MQTT_PASSWORD, TARGET_PREFIX]):
    print("Error: Missing configuration in .env file.")
    print("Please check your .env file for all required variables:")
    print("MQTT_BROKER, MQTT_PORT, MQTT_TOPIC, MQTT_USERNAME, MQTT_PASSWORD, TARGET_PREFIX")
    exit(1) # Exit the script if secrets are missing

# --- (The rest of the script is exactly the same) ---

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Successfully connected to {MQTT_BROKER}")
        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"Failed to connect, return code {rc}")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('utf-8')
        print(f"\n[Raw Message Received] {payload}")

        parts = payload.split('@', 1)

        if len(parts) == 2:
            prefix = parts[0].strip()
            data = parts[1].strip()

            # The script now uses the TARGET_PREFIX variable loaded from .env
            if prefix == TARGET_PREFIX:
                print(f"*** MATCH FOUND ***")
                print(f"Data: {data}")
                
                try:
                    with open(OUTPUT_FILE, 'a') as f:
                        f.write(data + '\n')
                    print(f"-> Successfully written to {OUTPUT_FILE}")
                except Exception as e:
                    print(f"!!! Error writing to file: {e}")
                    
            else:
                print(f"-> No match (Prefix was: {prefix})")
        else:
            print(f"-> Message format error (missing '@'): {payload}")

    except Exception as e:
        print(f"Error processing message: {e}")

# Main script execution
if __name__ == "__main__":
    
    print(f"Starting script. Matching data will be saved to {OUTPUT_FILE}")
    print(f"Monitoring for prefix: {TARGET_PREFIX}") # Added this line for clarity
    
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        print(f"Connecting to {MQTT_BROKER}:{MQTT_PORT}...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_forever()

    except KeyboardInterrupt:
        print("\nDisconnecting from broker...")
        client.disconnect()
        print("Script terminated.")
    except Exception as e:
        print(f"An error occurred: {e}")