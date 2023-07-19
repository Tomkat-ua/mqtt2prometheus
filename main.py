
from datetime import datetime
import yaml
import random
from paho.mqtt import client as mqtt_client
from prometheus_client import start_http_server, Gauge
from time import sleep as sleep
########### TEST ###################################
import confuse
# import argparse
config = confuse.Configuration('MQTT-Prom', __name__)
config.set_file('./config.yml')
# value = config['config'][2]['param1'].get()
int_value = config['config']['param2'].get(int)
str_value = config['config']['mqtt']['host'].get()
print(int_value)
print(str_value)

######

######Clases -----------------------
class Device:
    device_name = None
class Sensor:
    sensor_name = None
class Topic:
    name = None
    raw = None
    data = None

# class Config(object):
#     def __init__(self):
#         self.left = None
#         self.right = None
#         self.data = None
#         self.mqtt = None
#         # self.mqtt.port = None
#         # self.mqtt.user = None
#         # self.mqtt.password = None

#### GLOBAL Vars --------------------
appver = "0.0.3"
appname = "MQTT to Prometheus metrics extractor"
appshortname = "MQTT-Prom"
print(appname + " ver. "+appver)
env = 'dev' #argv[1]

http_port = None
get_delay = 5
broker = None
broker_port = None
username = None
password = None
topic_pattern = None
parsing_index_device = None
parsing_index_sensor = None
metric_sensor_name = None
metric_sensor_help = None
metric_sensor_topic = None
metric_sensor_index = None
metric_device_name = None
metric_device_help = None
metric_device_topic = None
metric_device_index = None
loging_index = None
loging_topic = None
client_id = f'python-mqtt-{random.randint(0, 100)}'


####------------------------------------------------

#####---------------------------------------------------
APP_INFO = Gauge('app_info', 'Return app info',['appname','appshortname','version','env'])
APP_CONFIG = Gauge('app_config', 'Return app config',['http_port','get_delay','broker','broker_port','topic_pattern'])
APP_INFO.labels(appname,appshortname,appver,env).set(1)

def config_read():
    with open('config.yml', 'r') as file:
        config_file = yaml.safe_load(file)
        # config_file = yaml.load(file, Loader=yaml.FullLoader)

    globals()["broker"] = config_file['config']['mqtt']['host']
    globals()["broker_port"] = config_file['config']['mqtt']['port']
    globals()["username"] = config_file['config']['mqtt']['user']
    globals()["password"] = config_file['config']['mqtt']['password']
    globals()["topic_pattern"] = config_file['config']['mqtt']['topic_path']
    globals()["http_port"] = config_file['config']['http']['port']
    globals()["parsing_index_device"] = config_file['config']['parsing']['index_device']
    globals()["parsing_index_sensor"] = config_file['config']['parsing']['index_sensor']
    globals()["parsing_index_sensor"] = config_file['config']['parsing']['index_sensor']

    globals()["metric_sensor_name"] = config_file['config']['metric']['sensor']['name']
    globals()["metric_sensor_help"] = config_file['config']['metric']['sensor']['help']
    globals()["metric_sensor_topic"] = config_file['config']['metric']['sensor']['topic']
    globals()["metric_sensor_index"] = config_file['config']['metric']['sensor']['index']

    globals()["metric_device_name"] = config_file['config']['metric']['device']['name']
    globals()["metric_device_help"] = config_file['config']['metric']['device']['help']
    globals()["metric_device_topic"] = config_file['config']['metric']['device']['topic']
    globals()["metric_device_index"] = config_file['config']['metric']['device']['index']

    globals()["loging_index"] = config_file['config']['loging']['index']
    globals()["loging_topic"] = config_file['config']['loging']['topic']

    print(config_file)
    file.close()
    print('-----------------------------------------------------')
    # print('broker:',broker,':',broker_port)
    # print('topic_pattern:',topic_pattern)
    # print('http_port:',http_port)
    # print('-----------------------------------------------------')

def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, broker_port)
    return client

def subscribe(client: mqtt_client):
    topic = Topic()
    device = Device()
    sensor = Sensor()

    def check_float(s) -> bool:
        try:
            s = float(s)
        except:
            return False
        return True

    def logformer(s):
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
        print (date_time+', '+s)

    def on_message_data(client, userdata, msg):
        topic.raw = msg.topic
        topic.name = topic.raw.replace("-", "_")
        topic.name = topic.name.split("/")
        topic.data = msg.payload.decode()
        device.device_name = topic.name[parsing_index_device]

        if topic.name[loging_index] == loging_topic:
            logformer(device.device_name +', '+ topic.data) # if  topic content log only
        if topic.name[metric_sensor_index] == metric_sensor_topic: # if topic contend - data
            sensor.sensor_name = topic.name[parsing_index_sensor]
            logformer(device.device_name +', ' +sensor.sensor_name + ', ' + topic.data)
            if check_float(topic.data): # if data - from sensor
                METRICA_SENSOR.labels(device.device_name,sensor.sensor_name).set(topic.data)
            else:  #if data - from device
                METRICA_DEVICE.labels(device.device_name,sensor.sensor_name,topic.data).set(0)

    client.subscribe(topic_pattern)
    client.on_message = on_message_data
    sleep(get_delay)

def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

if __name__ == '__main__':
    config_read()
    METRICA_SENSOR = Gauge(metric_sensor_name, metric_sensor_help, ['device', 'sensor'])
    METRICA_DEVICE = Gauge(metric_device_name, metric_device_help, ['device', 'sensor', 'value'])
    APP_CONFIG.labels(http_port, get_delay, broker, broker_port, topic_pattern).set(1)
    try:
        start_http_server(http_port)
    except Exception as e: print(e)
    while True:
        run()

