import minimalmodbus
import time
import requests
import os
import logging
import json
from datetime import datetime
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from paho.mqtt import client as mqtt_client

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(
            filename="logs/must_inverter_monitor.log",
            maxBytes=1_000_000,
            backupCount=10,
        ),
    ],
)

SERPORT = os.getenv("SERPORT")
SERTIMEOUT = float(os.getenv("SERTIMEOUT"))
SERBAUD = int(os.getenv("SERBAUD"))

INTERVAL = int(os.getenv("INTERVAL"))

MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC")
MQTT_DEVICE_NAME = os.getenv("MQTT_DEVICE_NAME")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID")

MAX_RETRIES = int(os.getenv("MAX_RETRIES"))

# Registers to retrieve data for
register_map = {
    25201: [
        "workState",
        "Work state",
        1,
        "map",
        {
            0: "PowerOn",
            1: "SelfTest",
            2: "OffGrid",
            3: "Grid-Tie",
            4: "Bypass",
            5: "Stop",
            6: "Grid Charging",
        },
    ],
    25205: ["batteryVoltage", "Battery voltage", 0.1, "V"],
    25206: ["inverterVoltage", "Inverter voltage", 0.1, "V"],
    25207: ["gridVoltage", "Grid voltage", 0.1, "V"],
    25208: ["busVoltage", "BUS voltage", 0.1, "V"],
    25209: ["controlCurrent", "Control current", 0.1, "A"],
    25210: ["inverterCurrent", "Inverter current", 0.1, "A"],
    25211: ["gridCurrent", "Grid current", 0.1, "A"],
    25212: ["loadCurrent", "Load current", 0.1, "A"],
    25213: ["inverterPower", "Inverter power(P)", 1, "W"],
    25214: ["gridPower", "Grid power(P)", 1, "W"],
    25215: ["loadPower", "Load power(P)", 1, "W"],
    25216: ["loadPercent", "Load percent", 1, "%"],
    25217: ["inverterComplexPower", "Inverter complex power(S)", 1, "VA"],
    25218: ["gridComplexPower", "Grid complex power(S)", 1, "VA"],
    25219: ["loadComplexPower", "Load complex power(S)", 1, "VA"],
    25221: ["inverterReactivePower", "Inverter reactive power(Q)", 1, "var"],
    25222: ["gridReactivePower", "Grid reactive power(Q)", 1, "var"],
    25223: ["loadReactivePower", "Load reactive power(Q)", 1, "var"],
    25225: ["inverterFrequency", "Inverter frequency", 0.01, "Hz"],
    25226: ["gridFrequency", "Grid frequency", 0.01, "Hz"],
    25233: ["acRadiatorTemperature", "AC radiator temperature", 1, "C"],
    25234: ["transformerTemperature", "Transformer temperature", 1, "C"],
    25235: ["dcRadiatorTemperature", "DC radiator temperature", 1, "C"],

    25237: ["InverterRelayState", "InverterRelayState", 1, "on/off"],
    25238: ["GridRelayState", "GridRelayState", 1, "on/off"],
    25239: ["LoadRelayState", "LoadRelayState", 1, "on/off"],
    25240: ["N_LineRelayState", "N_LineRelayState", 1, "on/off"],
    25241: ["DCRelayState", "DCRelayState", 1, "on/off"],
    25242: ["EarthRelayState", "EarthRelayState", 1, "on/off"],

    25245: ["AccumulatedChargerPowerM", "AccumulatedChargerPowerM", 1, "MWh"],
    25246: ["AccumulatedChargerPower", "AccumulatedChargerPower", 1, "kWh"],

    25247: ["AccumulatedDischargerPowerM", "AccumulatedDischargerPowerM", 1, "MWh"],
    25248: ["AccumulatedDischargerPower", "AccumulatedDischargerPower", 1, "kWh"],

    25249: ["AccumulatedBuyPowerM", "AccumulatedBuyPowerM", 1, "MWh"],
    25250: ["AccumulatedBuyPower", "AccumulatedBuyPower", 1, "kWh"],

    25251: ["AccumulatedSellPowerM", "AccumulatedSellPowerM", 1, "MWh"],
    25252: ["AccumulatedSellPower", "AccumulatedSellPower", 1, "kWh"],

    25253: ["AccumulatedLoadPowerM", "AccumulatedLoadPowerM", 1, "MWh"],
    25254: ["AccumulatedLoadPower", "AccumulatedLoadPower", 1, "kWh"],

    25255: ["AccumulatedSelfUsePowerM", "AccumulatedSelfUsePowerM", 1, "MWh"],
    25256: ["AccumulatedSelfUsePower", "AccumulatedSelfUsePower", 1, "kWh"],

    25257: ["AccumulatedPvSellPowerM", "AccumulatedPvSellPowerM", 1, "MWh"],
    25258: ["AccumulatedPvSellPower", "AccumulatedPvSellPower", 1, "kWh"],

    25259: ["AccumulatedGridChargerPowerM", "AccumulatedGridChargerPowerM", 1, "MWh"],
    25260: ["AccumulatedGridChargerPower", "AccumulatedGridChargerPower", 1, "kWh"],

    25261: ["InverterErrorMessage", "InverterErrorMessage", 1, ""],
    25265: ["InverterWarningMessage", "InverterWarningMessage", 1, ""],



    25273: ["batteryPower", "Battery power", 1, "W"],
    25274: ["batteryCurrent", "Battery current", 1, "A"],

    25279: ["ArrowFlag","Arrow Flag",1,""],
    20109: [
        "EnergyUseMode",
        "Energy use mode",
        1,
        "map",
        {
            0: "-",
            1: "SBU",
            2: "SUB",
            3: "UTI",
            4: "SOL",
        },
    ],

    20111: [
        "Grid_protect_standard",
        "Grid protect standard",
        1,
        "map",
        {
            0: "VDE4105",
            1: "UPS",
            2: "HOME",
            3: "GEN",
        },
    ],

    20112: [
        "SolarUseAim",
        "SolarUse Aim",
        1,
        "map",
        {
            0: "LBU",
            1: "BLU",
        },
    ],
    20113: ["Inv_max_discharger_cur", "Inverter max discharger current", 0.1, "A"],
    20118: ["BatStopDischargingV", "Battery stop discharging voltage", 0.1, "V"],
    20119: ["BatStopChargingV", "Battery stop charging voltage", 0.1, "V"],
    20125: ["GridMaxChargerCurSet", "Grid max charger current set", 0.1, "A"],
    20127: ["BatLowVoltage", "Battery low voltage", 0.1, "V"],
    20128: ["BatHighVoltage", "Battery high voltage", 0.1, "V"],


    15201: [
        "ChargerWorkstate",
        "Charger Workstate",
        1,
        "map",
        {
            0: "Initialization",
            1: "Selftest",
            2: "Work",
            3: "Stop",
        },
    ],

    15202: [
        "MpptState",
        "Mppt State",
        1,
        "map",
        {
            0: "Stop",
            1: "MPPT",
            2: "Current limiting",
        },
    ],

    15203: [
        "ChargingState",
        "Charging State",
        1,
        "map",
        {
            0: "Stop",
            1: "Absorb charge",
            2: "Float charge",
            3: "Equalization charge",
        },
    ],

  15205: ["PvVoltage", "Pv. Voltage", 0.1, "V"],
  15206: ["chBatteryVoltage", "Ch. Battery Voltage", 0.1, "V"],
  15207: ["chChargerCurrent", "Ch. Charger Current", 0.1, "A"],
  15208: ["ChargerPower", "Ch. Charger Power", 1, "W"],
  15209: ["RadiatorTemperature", "Ch. Radiator Temperature", 1, "C"],
  15210: ["ExternalTemperature", "Ch. External Temperature", 1, "C"],
  15211: ["BatteryRelay", "Battery Relay", 1, ""],
  15212: ["PvRelay", "Pv. Relay", 1, ""],
  15213: ["ChargerErrorMessage", "Charger Error Message", 1, ""],
  15214: ["ChargerWarningMessage", "Charger Warning Message", 1, ""],
  15215: ["BattVolGrade", "BattVolGrade", 1, "V"],
  15216: ["RatedCurrent", "Rated Current", 0.1, "A"],

  15217: ["AccumulatedPowerM", "Accumulated PowerM", 1 , "MWh"],
  15218: ["AccumulatedPower", "Accumulated Power", 1 , "kWh"],
  15219: ["AccumulatedTimeDay", "Accumulated Time day", 1 , "d"],

# Get data from External BMS
  109: ["BMS_Battery_Voltage", "BMS Battery Voltage", 0.1 , "V"],
  110: ["BMS_Battery_Current", "BMS Battery Current", 0.1 , "A"],
  111: ["BMS_Battery_Temperature", "BMS Battery Temperature", 1 , "C"],
  112: ["BMS_Battery_Errors", "BMS Battery Error", 1 , ""],
  113: ["BMS_Battery_SOC", "BMS Battery SOC", 1 , "%"],
  114: ["BMS_Battery_SOH", "BMS Battery SOH", 1 , "%"]

}

properties = {
    'workState': {'name': 'Work state', 'format': '', 'icon': 'solar-power'},
    'batteryVoltage': {'name': 'Battery voltage', 'format': 'V'},
    'inverterVoltage': {'name': 'Inverter voltage', 'format': 'V'},
    'gridVoltage': {'name': 'Grid voltage', 'format': 'V'},
    'busVoltage': {'name': 'BUS voltage', 'format': 'V'},
    'controlCurrent': {'name': 'Control current', 'format': 'A'},
    'inverterCurrent': {'name': 'Inverter current', 'format': 'A'},
    'gridCurrent': {'name': 'Grid current', 'format': 'A'},
    'loadCurrent': {'name': 'Load current', 'format': 'A'},
    'inverterPower': {'name': 'Inverter power(P)', 'format': 'W'},
    'gridPower': {'name': 'Grid power(P)', 'format': 'W'},
    'loadPower': {'name': 'Load power(P)', 'format': 'W'},
    'loadPercent': {'name': 'Load percent', 'format': '%'},
    'inverterComplexPower': {'name': 'Inverter complex power(S)', 'format': 'VA'},
    'gridComplexPower': {'name': 'Grid complex power(S)', 'format': 'VA'},
    'loadComplexPower': {'name': 'Load complex power(S)', 'format': 'VA'},
    'inverterReactivePower': {'name': 'Inverter reactive power(Q)', 'format': ''},
    'gridReactivePower': {'name': 'Grid reactive power(Q)', 'format': ''},
    'loadReactivePower': {'name': 'Load reactive power(Q)', 'format': ''},
    'inverterFrequency': {'name': 'Inverter frequency', 'format': 'Hz'},
    'gridFrequency': {'name': 'Grid frequency', 'format': 'Hz'},
    'acRadiatorTemperature': {'name': 'AC radiator temperature', 'format': '°C'},
    'transformerTemperature': {'name': 'Transformer temperature', 'format': '°C'},
    'dcRadiatorTemperature': {'name': 'DC radiator temperature', 'format': '°C'},
    'InverterRelayState': {'name': 'InverterRelayState', 'format': ''},
    'GridRelayState': {'name': 'GridRelayState', 'format': ''},
    'LoadRelayState': {'name': 'LoadRelayState', 'format': ''},
    'N_LineRelayState': {'name': 'N_LineRelayState', 'format': ''},
    'DCRelayState': {'name': 'DCRelayState', 'format': ''},
    'EarthRelayState': {'name': 'EarthRelayState', 'format': ''},
    'AccumulatedChargerPowerM': {'name': 'AccumulatedChargerPowerM', 'format': 'Wh'},
    'AccumulatedChargerPower': {'name': 'AccumulatedChargerPower', 'format': 'kWh'},
    'AccumulatedDischargerPowerM': {'name': 'AccumulatedDischargerPowerM', 'format': 'Wh'},
    'AccumulatedDischargerPower': {'name': 'AccumulatedDischargerPower', 'format': 'kWh'},
    'AccumulatedBuyPowerM': {'name': 'AccumulatedBuyPowerM', 'format': 'Wh'},
    'AccumulatedBuyPower': {'name': 'AccumulatedBuyPower', 'format': 'kWh'},
    'AccumulatedSellPowerM': {'name': 'AccumulatedSellPowerM', 'format': 'Wh'},
    'AccumulatedSellPower': {'name': 'AccumulatedSellPower', 'format': 'kWh'},
    'AccumulatedLoadPowerM': {'name': 'AccumulatedLoadPowerM', 'format': 'Wh'},
    'AccumulatedLoadPower': {'name': 'AccumulatedLoadPower', 'format': 'kWh'},
    'AccumulatedSelfUsePowerM': {'name': 'AccumulatedSelfUsePowerM', 'format': 'Wh'},
    'AccumulatedSelfUsePower': {'name': 'AccumulatedSelfUsePower', 'format': 'kWh'},
    'AccumulatedPvSellPowerM': {'name': 'AccumulatedPvSellPowerM', 'format': 'Wh'},
    'AccumulatedPvSellPower': {'name': 'AccumulatedPvSellPower', 'format': 'kWh'},
    'AccumulatedGridChargerPowerM': {'name': 'AccumulatedGridChargerPowerM', 'format': 'Wh'},
    'AccumulatedGridChargerPower': {'name': 'AccumulatedGridChargerPower', 'format': 'kWh'},
    'batteryPower': {'name': 'Battery power', 'format': 'W'},
    'batteryCurrent': {'name': 'Battery current', 'format': 'A'},
    'PvVoltage': {'name': 'Pv. Voltage', 'format': 'V'},
    'chBatteryVoltage': {'name': 'Ch. Battery Voltage', 'format': 'V'},
    'chChargerCurrent': {'name': 'Ch. Charger Current', 'format': 'A'},
    'ChargerPower': {'name': 'Ch. Charger Power', 'format': 'W'},
    'RadiatorTemperature': {'name': 'Ch. Radiator Temperature', 'format': '°C'},
    'ExternalTemperature': {'name': 'Ch. External Temperature', 'format': '°C'},
    'BattVolGrade': {'name': 'BattVolGrade', 'format': 'V'},
    'RatedCurrent': {'name': 'Rated Current', 'format': 'A'},
    'AccumulatedPowerM': {'name': 'Accumulated PowerM', 'format': 'Wh'},
    'AccumulatedPower': {'name': 'Accumulated Power', 'format': 'kWh'},
    'AccumulatedTimeDay': {'name': 'Accumulated Time day', 'format': 'd'},
    'BMS_Battery_Voltage': {'name': 'BMS Battery Voltage', 'format': 'V'},
    'BMS_Battery_Current': {'name': 'BMS Battery Current', 'format': 'A'},
    'BMS_Battery_Temperature': {'name': 'BMS Battery Temperature', 'format': '°C'},
    'BMS_Battery_SOC': {'name': 'BMS Battery SOC', 'format': '%'},
    'BMS_Battery_SOH': {'name': 'BMS Battery SOH', 'format': '%'},
    'EnergyUseMode': {'name': 'Energy use mode', 'format': ''},
    'Grid_protect_standard': {'name': 'Grid protect standard', 'format': ''},
    'SolarUseAim': {'name': 'SolarUse Aim', 'format': ''},
    'ChargerWorkstate': {'name': 'Charger Workstate', 'format': ''},
    'MpptState': {'name': 'Mppt State', 'format': ''},
    'ChargingState': {'name': 'Charging State', 'format': ''}
}

def read_register_values(i, startreg, count):
    stats_line = ""
    register_id = startreg

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            results = i.read_registers(startreg, count)
            break
        except minimalmodbus.InvalidResponseError:
            logging.info(f"Ivalid response when retry attempt `{attempt}` of `{count}` bytes from `{startreg}`")
            time.sleep(INTERVAL)
        except minimalmodbus.NoResponseError:
            logging.info(f"No response when read retry attempt `{attempt}` of `{count}` bytes from `{startreg}`")
            time.sleep(INTERVAL)
    for r in results:
        if register_id in register_map:
            r_key = register_map[register_id][0]
            r_unit = register_map[register_id][2]

            if register_map[register_id][3] == "map":
                r_value = register_map[register_id][4][r]
            else:
                r_value = str(round(r * r_unit, 2))

            # convert from offset val fix for Inverter power owerload
            if register_id == 25213 or register_id == 25273 or register_id == 25274  or register_id == 25214 or register_id == 110 :
                if float(r_value) > 32000 :
                   r_value =- abs(float(r_value)- 65536)


            stats_line += r_key + "=" + str(r_value) + ","

        register_id += 1

    # Remove comma at the end
    stats_line = stats_line[:-1]

    return stats_line

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT Broker!")
        else:
            logging.warning(f"Failed to connect, return code {rc}")

    client = mqtt_client.Client(MQTT_CLIENT_ID)
    client.on_connect = on_connect
    client.connect(MQTT_BROKER, MQTT_PORT)
    return client

def publish(client, key, value):
    topic = f"{MQTT_TOPIC}/sensor/{MQTT_DEVICE_NAME}_{key}"
    result = client.publish(topic, value)
    status = result[0]
    if status == 0:
        logging.info(f"Send `{value}` to topic `{topic}`")
    else:
        logging.warning(f"Failed to send message to topic `{topic}`")

def send_data(stats):
    props = stats.split(",")
    for propObj in props:
      prop = propObj.split("=")
      publish(client, prop[0], prop[1])

def register_inverter():
    topic = f"{MQTT_TOPIC}/sensor/{MQTT_DEVICE_NAME}/config"
    config = {
        "name": MQTT_DEVICE_NAME,
        "state_topic": f"{MQTT_TOPIC}/sensor/{MQTT_DEVICE_NAME}"
    }

    result = client.publish(topic, json.dumps(config, indent=4))
    status = result[0]

    if status == 0:
        logging.info(f"Register inverter `{MQTT_DEVICE_NAME}`")
    else:
        logging.warning(f"Failed to register inverter `{MQTT_DEVICE_NAME}`")

def register_topic(property_name):
    topic_base = f"{MQTT_TOPIC}/sensor/{MQTT_DEVICE_NAME}_{property_name}"
    topic = f"{topic_base}/config"
    prop = properties[property_name]

    try:
        icon = prop['icon']
    except KeyError:
        icon = None

    config = {
        "object_id": f"{MQTT_DEVICE_NAME}_{property_name}",
        "name": prop['name'],
        "unit_of_measurement": prop['format'],
        "state_topic": topic_base
    }

    if icon is not None:
        config['icon'] = f"mdi:{icon}"

    result = client.publish(topic, json.dumps(config, indent=4))
    status = result[0]

    if status == 0:
        logging.info(f"Register topic `{topic_base}`")
    else:
        logging.warning(f"Failed to register topic `{topic_base}`")

def register_topics():
    for key, value in properties.items():
        register_topic(key)

infinite = True

client = connect_mqtt()
client.loop_start()

register_inverter()
time.sleep(INTERVAL)
register_topics()

while infinite:
    i = minimalmodbus.Instrument(SERPORT, 4)
    i.serial.timeout = SERTIMEOUT
    i.serial.baudrate = SERBAUD

    stats = []

    stats.append(read_register_values(i, 109, 6))
    stats.append(read_register_values(i, 15201, 20))
    stats.append(read_register_values(i, 20101, 114))
    stats.append(read_register_values(i, 25201, 80))

    send_data(",".join(stats))

    # infinite = False

    if infinite:
        time.sleep(INTERVAL)

if not infinite:
    client.loop_stop()
    client.disconnect()
