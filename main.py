
from datetime import datetime
import yaml
import random
from paho.mqtt import client as mqtt_client
from prometheus_client import start_http_server, Gauge
from time import sleep as sleep

######
# with open('config.yml', 'r') as file:
#     config_file = yaml.safe_load(file)
#     # config_file = yaml.load(file, Loader=yaml.FullLoader)
#     print(config_file['config']['mqtt'])
#     print(config_file['config']['mqtt']['host'])
# class Config
#
######Clases -----------------------
class Device:
    device_name=''
class Sensor:
    sensor_name=''
class Topic:
    name=''
    raw=''
    data=''



#### GLOBAL Vars --------------------
appver = "0.0.1"
appname = "MQTT to Prometheus metrics extractor"
appshortname = "MQTT-Prom"
print(appname + " ver. "+appver)
env = 'dev' #argv[1]

http_port = ''
get_delay = 5
broker = ''
broker_port = ''
username = ''
password = ''
topic_pattern = ''
parsing_index_device = ''
parsing_index_sensor = ''
metric_name = ''
metric_help = ''
metric_topic = ''
metric_index = ''
loging_index = ''
loging_topic = ''
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
    globals()["metric_name"] = config_file['config']['metric']['name']
    globals()["metric_help"] = config_file['config']['metric']['help']
    globals()["metric_topic"] = config_file['config']['metric']['topic']
    globals()["metric_index"] = config_file['config']['metric']['index']
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
            logformer(device.device_name +', '+ topic.data)
        if topic.name[metric_index] == metric_topic:
            sensor.sensor_name = topic.name[parsing_index_sensor]
            logformer(device.device_name +', ' +sensor.sensor_name + ', ' + topic.data)
            if check_float(topic.data):
                METRICA_SENSOR.labels(device.device_name,sensor.sensor_name,'').set(topic.data)
            else: METRICA_SENSOR.labels(device.device_name,sensor.sensor_name,topic.data).set(0)

    client.subscribe(topic_pattern)
    client.on_message = on_message_data
    sleep(get_delay)

def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

if __name__ == '__main__':
    config_read()
    METRICA_SENSOR = Gauge(metric_name, metric_help, ['device', 'sensor', 'data'])
    APP_CONFIG.labels(http_port, get_delay, broker, broker_port, topic_pattern).set(1)
    try:
        start_http_server(http_port)
    except Exception as e: print(e)
    while True:
        run()

