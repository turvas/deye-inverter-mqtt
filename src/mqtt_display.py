# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "colorama",
#     "paho-mqtt",
#     "prettytable",
#     "python-dotenv",
# ]
# ///

import os
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from prettytable import PrettyTable
from datetime import datetime
#import json
import threading
import time
from pathlib import Path
from colorama import Fore, Style, init

# Load environment variables from config.env
# C:/Users/kalev/projects/akuu-energy-v3/backend-python/deye-inverter-mqtt/src/
script_dir = Path(__file__).parent
load_dotenv(script_dir / "config.env", override=False)

MQTT_HOST = os.getenv('MQTT_HOST')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1883))
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')
MQTT_TOPIC_PREFIX = os.getenv('MQTT_TOPIC_PREFIX')

# Data structure to hold the latest received data for each object
# {
#   object_number: {
#     'time': value,
#     'power': value,
#     'soc': value,
#     'enabled': value,
#     'voltage': value
#   },
#   ...\
# }
data_store = {i: {param: {'current': None, 'previous': None} for param in ['time', 'power', 'soc', 'enabled', 'voltage']} for i in range(1, 7)}
# Data structure to hold general metrics
general_metrics_store = {
    'battery/power': {'current': None, 'previous': None},
    'battery/soc': {'current': None, 'previous': None},
    'ac/total_power': {'current': None, 'previous': None},
    'ac/ups/total_power': {'current': None, 'previous': None},
    'settings/battery/maximum_charge_current': {'current': None, 'previous': None},
    'settings/battery/maximum_discharge_current': {'current': None, 'previous': None},
    'settings/battery/maximum_grid_charge_current': {'current': None, 'previous': None},
    'settings/battery/grid_charge': {'current': None, 'previous': None},
    'settings/workmode': {'current': None, 'previous': None},
    'settings/solar_sell': {'current': None, 'previous': None},
}

# Mapping for friendly names of general metrics
METRIC_NAME_MAPPING = {
    'battery/power': 'Battery Power',
    'battery/soc': 'Battery SoC',
    'ac/total_power': 'Grid Consumption Power',
    'ac/ups/total_power': 'UPS Load Power',
    'settings/battery/maximum_charge_current': 'Max Charge Current',
    'settings/battery/maximum_discharge_current': 'Max Discharge Current',
    'settings/battery/maximum_grid_charge_current': 'Max Grid Charge Current',
    'settings/battery/grid_charge': 'Grid Charge Enabled',
    'settings/workmode': 'Work Mode',
    'settings/solar_sell': 'Solar Sell Enabled',
}

last_update_time = None
data_lock = threading.Lock()

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"Connected with result code {rc}")
    if rc == 0:
        base_topic = f"{MQTT_TOPIC_PREFIX}/timeofuse"
        # Subscribe to all relevant topics
        for obj_num in range(1, 7):
            for param in ['time', 'power', 'soc', 'enabled', 'voltage']:
                topic = f"{base_topic}/{param}/{obj_num}"
                client.subscribe(topic)
                print(f"Subscribed to {topic}")

        # Subscribe to general metrics topics
        general_topics = [
            'battery/power',
            'battery/soc',
            'ac/total_power',
            'ac/ups/total_power',
            'settings/battery/maximum_charge_current',
            'settings/battery/maximum_discharge_current',
            'settings/battery/maximum_grid_charge_current',
            'settings/battery/grid_charge',
            'settings/workmode',
            'settings/solar_sell',
        ]
        for gt in general_topics:
            topic = f"{MQTT_TOPIC_PREFIX}/{gt}"
            client.subscribe(topic)
            print(f"Subscribed to {topic}")
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    global last_update_time
    try:
        topic_parts = msg.topic.split('/')
        if len(topic_parts) >= 3 and topic_parts[-3] == 'timeofuse':
            process_timeofuse(topic_parts, msg.payload.decode())
        elif msg.topic.startswith(f"{MQTT_TOPIC_PREFIX}/"):
            # Check for general metrics
            suffix = msg.topic[len(f"{MQTT_TOPIC_PREFIX}/"):]
            if suffix in general_metrics_store:
                process_general_metric(suffix, msg.payload.decode())
            else:
                print(f"Received message on unhandled topic: {msg.topic}")
        else:
            print(f"Received message on unexpected topic: {msg.topic}")

    except Exception as e:
        print(f"Error processing message: {e}")
        print(f"Topic: {msg.topic}, Payload: {msg.payload.decode()}")


def process_timeofuse(topic_parts: list, payload: str):
    global last_update_time
    object_param = topic_parts[-2]
    object_number = int(topic_parts[-1])
    value = payload
    with data_lock:
        if object_number in data_store:
            # Attempt to convert to appropriate type
            if object_param in ['power', 'soc', 'voltage', 'time']:
                try:
                    value = int(float(value))
                except ValueError:
                    value = value # keep as string if conversion fails
            elif object_param == 'enabled':
                try:
                    value = bool(int(float(value)))  # Assuming '0' or '1' for boolean
                except ValueError:
                    value = value # keep as string if conversion fails

            # Update historic data
            current_param_data = data_store[object_number][object_param]
            current_param_data['previous'] = current_param_data['current']
            current_param_data['current'] = value
            last_update_time = datetime.now()
        else:
            print(f"Received data for unknown object number: {object_number}")

def process_general_metric(metric_key, value_str):
    global last_update_time
    with data_lock:
        try:
            # Attempt to convert to appropriate type
            if metric_key in ['battery/power', 'battery/soc', 'ac/total_power', 'ac/ups/total_power',
                             'settings/battery/maximum_charge_current', 'settings/battery/maximum_discharge_current',
                             'settings/battery/maximum_grid_charge_current']:
                value = float(value_str)
            elif metric_key in ['settings/battery/grid_charge', 'settings/solar_sell']:
                value = bool(int(float(value_str))) # Assuming '0' or '1'
            elif metric_key == 'settings/workmode':
                value = value_str # Keep as string
            else:
                value = value_str # Default to string for unknown types

            # Update historic data
            current_metric_data = general_metrics_store[metric_key]
            current_metric_data['previous'] = current_metric_data['current']
            current_metric_data['current'] = value
            last_update_time = datetime.now()
        except ValueError:
            print(f"Could not convert value '{value_str}' for metric '{metric_key}'. Keeping as string.")
            general_metrics_store[metric_key]['previous'] = general_metrics_store[metric_key]['current']
            general_metrics_store[metric_key]['current'] = value_str


def colorize_value(current_value, previous_value):
    if current_value is None:
        return 'N/A'
    if previous_value is None or not isinstance(current_value, (int, float)) or not isinstance(previous_value, (int, float)):
        return str(current_value)

    if current_value > previous_value:
        return f"{Fore.GREEN}{current_value}{Style.RESET_ALL}"
    elif current_value < previous_value:
        return f"{Fore.RED}{current_value}{Style.RESET_ALL}"
    else:
        return str(current_value)

def display_table():
    while True:
        os.system('cls' if os.name == 'nt' else 'clear') # Clear console
        table = PrettyTable()
        table.field_names = ["Object Number", "Time", "Power", "SoC", "Enabled", "Voltage"]

        with data_lock:
            print("General Metrics:")
            general_metrics_table = PrettyTable()
            general_metrics_table.field_names = ["Metric", "Value"]
            for key in sorted(general_metrics_store.keys()):
                metric_data = general_metrics_store[key]
                colored_value = colorize_value(metric_data['current'], metric_data['previous'])
                friendly_name = METRIC_NAME_MAPPING.get(key)
                if not friendly_name:  # fallback just in case
                    friendly_name = key
                general_metrics_table.add_row([friendly_name, colored_value])
            print(general_metrics_table)
            print("\n" + "="*50 + "\n") # Separator

            print("Time of Use Objects Data")
            for obj_num in sorted(data_store.keys()):
                obj_data = data_store[obj_num]
                table.add_row([\
                    obj_num,\
                    colorize_value(obj_data['time']['current'], obj_data['time']['previous']),\
                    colorize_value(obj_data['power']['current'], obj_data['power']['previous']),\
                    colorize_value(obj_data['soc']['current'], obj_data['soc']['previous']),\
                    colorize_value(obj_data['enabled']['current'], obj_data['enabled']['previous']),\
                    colorize_value(obj_data['voltage']['current'], obj_data['voltage']['previous'])\
                ])

            print(table)
            if last_update_time:
                print(f"\nLast Updated: {last_update_time.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                print("\nWaiting for data...")

        time.sleep(1) # Refresh every 1 second

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    init() # Initialize Colorama
    try:
        client.connect(MQTT_HOST, MQTT_PORT, 60)
    except Exception as e:
        print(f"Could not connect to MQTT broker: {e}")
        return

    # Start the MQTT client loop in a separate thread to handle incoming messages
    mqtt_thread = threading.Thread(target=client.loop_forever)
    mqtt_thread.daemon = True
    mqtt_thread.start()

    # Start the display table thread
    display_thread = threading.Thread(target=display_table)
    display_thread.daemon = True
    display_thread.start()

    # Keep the main thread alive to allow daemon threads to run
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nExiting application.")
        client.disconnect()

if __name__ == "__main__":
    main()
