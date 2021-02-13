import json
import sys
import logging
from logging.handlers import RotatingFileHandler
import os 
import configparser
from queue import Queue
from threading import Thread
from influxconnector.client import InfluxClient
from influxconnector.convertor.ppmp.v3.measurement import PPMPMeasurement as PPMPMeasurementV3
from influxconnector.convertor.ppmp.v3.process import PPMPProcess as PPMPProcessV3
from influxconnector.convertor.ppmp.v3.machine import PPMPMachine as PPMPMachineV3
from mqttconnector.client import MQTTClient
from time import time
from pathlib import Path
from tomlconfig.tomlutils import TomlParser

PROJECT_NAME = 'ppmp2influxconnector'

LOGFOLDER = "./logs/"

# configure logging
logger = logging.getLogger('root')
logger.setLevel(logging.INFO)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)

# fl = logging.FileHandler(f'{PROJECT_NAME}.log')
# Rotation file handler mit 200 000 bytes pro file und 10 files in rotation


try:  
    os.mkdir(LOGFOLDER)
    logger.info(f'create logfolder: {LOGFOLDER}')
except OSError as error:  
    logger.info(f'create logfolder: {LOGFOLDER}:{error}')

fl = RotatingFileHandler(f'{LOGFOLDER}{PROJECT_NAME}.log',mode='a', maxBytes=2*(10**5), backupCount=10)

fl.setLevel(logging.ERROR)
fl.setFormatter(formatter)
logger.addHandler(fl)

toml = TomlParser(f'{PROJECT_NAME}.toml')

MQTT_NETWORK_NAME = toml.get('mqtt.network_name', 'mh')
MQTT_TOPIC_PPMP = MQTT_NETWORK_NAME + '/+/' + 'ppmp'
MQTT_TOPIC_INFO = MQTT_NETWORK_NAME + '/+/' + 'ppmp'
MQTT_HOST = toml.get('mqtt.host', 'localhost')
MQTT_PORT = toml.get('mqtt.port', 1883)
MQTT_USERNAME = toml.get('mqtt.username', '')
MQTT_PASSWORD = toml.get('mqtt.password', '')
MQTT_TLS_CERT = toml.get('mqtt.tls_cert', '')

INFLUXDB_HOST = toml.get('influxdb.host', 'localhost')
INFLUXDB_PORT = toml.get('influxdb.port', 8086)
INFLUXDB_USERNAME = toml.get('influxdb.username', 'admin')
INFLUXDB_PASSWORD = toml.get('influxdb.password', '')
INFLUXDB_DB = toml.get('influxdb.db', 'ppmp')
INFLUXDB_FILTER = toml.get('influxdb.filter', [])
INFLUXDB_CACHING_PERIOD = toml.get('influxdb.caching_period', 1)

# influxdb.influxDuration =368h0m0s
INFLUXDB_DURATION_TIME = toml.get('influxdb.influxDuration', '')
# influxdb.shardGroupDuration = 168h0m0s
INFLUXDB_SHARDGROUPDURATION_TIME = toml.get('influxdb.shardGroupDuration', '')

# create InfluxDB and MQTT clients
INFLUX_CLIENT = InfluxClient()

# create distributed queues for all threads
QUEUE_MQTT = Queue(maxsize=0)
QUEUE_INFLUX = Queue(maxsize=0)


def main():

    # connect to MQTT and InfluxDB
    mqtt_client = MQTTClient(host=MQTT_HOST, port=MQTT_PORT,
                             user=MQTT_USERNAME, password=MQTT_PASSWORD, tls_cert=MQTT_TLS_CERT)
    mqtt_client.connect(forever=True)
    INFLUX_CLIENT.connect(host=INFLUXDB_HOST, port=INFLUXDB_PORT,
                          username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD, database=INFLUXDB_DB)
    setinflux_durationTime(influxDuration=INFLUXDB_DURATION_TIME,
                           influx_shardGroupDuration=INFLUXDB_SHARDGROUPDURATION_TIME)

    # create and start convertor thread
    Thread(target=convertor).start()

    # create and start InfluxDB consumer thread
    Thread(target=influx_consumer).start()

    # subscribe to MQTT and fill the queue
    mqtt_client.subscribe(MQTT_TOPIC_PPMP, mqtt_producer)
    # srw2ho: topic already used: mqtt_client.subscribe(MQTT_TOPIC_INFO, mqtt_producer)

    mqtt_client.start()


def mqtt_producer(payload):
    """ Subscribe to MQTT changes and write payload into queue

    Arguments:
        payload {obj} -- JSON payload in PPMP format
    """
    QUEUE_MQTT.put(payload)
    # logger.info(f'MQTT Queue size: {QUEUE_MQTT.qsize()}')


def convertor():
    """ Convert MQTT payload and put it into InfluxDB queue

    Arguments:
        payload {obj} -- MQTT payload to convert and put into queue
    """
    while True:
        payload = QUEUE_MQTT.get()

        # logger.info(f'InfluxDB convertor Queue size: {QUEUE_INFLUX.qsize()}')
        try:
            data = json.loads(payload)
            if 'content-spec' in data:
                ppmp_obj = None

                # differentiate payload via "content-spec" -> v2/v3
                # differentiate PPMP type (machine, process, measurement)
                if data['content-spec'] == 'urn:spec://eclipse.org/unide/process-message#v3':
                    ppmp_obj = PPMPProcessV3(data)
                if data['content-spec'] == 'urn:spec://eclipse.org/unide/measurement-message#v3':
                    ppmp_obj = PPMPMeasurementV3(data)
                if data['content-spec'] == 'urn:spec://eclipse.org/unide/machine-message#v3':
                    ppmp_obj = PPMPMachineV3(data)

                if ppmp_obj:
                    line_protocol = ppmp_obj.export_to_line_protocol()

                    # TODO do (complete) sanity check here
                    if line_protocol and line_protocol.split(' ')[-2]:
                        QUEUE_INFLUX.put(line_protocol)
                    else:
                        raise Exception(
                            f'Invalid Line Protocol format: {line_protocol} from PPMP {data}')
            else:
                raise Exception(f'Invalid PPMP format: {data}')
        except Exception as e:
            logger.error(e)


def influx_consumer():
    """ Wait for items in the queue and write them into InfluxDB
    """
    payload = []

    last_execution = time()
    while True:

        try:
            line = QUEUE_INFLUX.get()

            payload.append(line)

            timediff = time() - last_execution
            payload_length = len(payload)
            if payload_length >= 1000 or timediff > INFLUXDB_CACHING_PERIOD:
                logger.info(
                    f'InfluxDB database queue size: {payload_length}, wrote {int(payload_length / timediff)} entries/sec')

                INFLUX_CLIENT.write(payload, protocol='line')
                payload = []
                last_execution = time()

        except Exception as e:
            logger.error(payload)
            logger.error(e)
            # reset payload to not overflow queue -> will result in data loss!
            payload = []

        finally:
            QUEUE_INFLUX.task_done()


def readRetentions():
    retentionlist = INFLUX_CLIENT.get_list_retention_policies(
        database=INFLUXDB_DB)
    retentionduration = None
    retentionshardGroupDuration = None
    retentionlistdict = {item['name']: item for item in retentionlist}
    return retentionlistdict
   


def setinflux_durationTime(influxDuration=None, influx_shardGroupDuration=None):
    try:
        retentionlistdict = readRetentions()
        
        retentionduration=None
        retentionshardGroupDuration = None

        if retentionlistdict['autogen'] != None:
            for key, value in retentionlistdict['autogen'].items():
                if key == 'duration':
                    retentionduration = value
                if key == 'shardGroupDuration':
                    retentionshardGroupDuration = value
                formatedtxt = "{dbase}-retention-autogen: {key} : {value}".format(dbase = INFLUXDB_DB, key = key, value=value )
                logger.info(formatedtxt)
                # for testing logger.error(formatedtxt)

        if ((influxDuration == '') and (influx_shardGroupDuration == '')):
            return

        if ((influxDuration == None) and (influx_shardGroupDuration == None)):
            return

        if (retentionduration != None) and (retentionshardGroupDuration != None):

            if influxDuration == '':
                influxDuration = retentionduration

            if influx_shardGroupDuration == '':
                influx_shardGroupDuration = retentionshardGroupDuration

            retentionset = False

            if (retentionduration != influxDuration) or (retentionshardGroupDuration != influx_shardGroupDuration):
                INFLUX_CLIENT.alter_retention_policy(
                        name="autogen", database=INFLUXDB_DB, duration=influxDuration, shard_duration=influx_shardGroupDuration)
                retentionset=True

            if retentionset:
                retentionlistdict = readRetentions()
                if retentionlistdict['autogen'] != None:
                    for key, value in retentionlistdict['autogen'].items():
                        formatedtxt = "{dbase}-retention-autogen-Set: {key} : {value}".format(dbase = INFLUXDB_DB, key = key, value=value )
                        logger.info(formatedtxt)
                        # for testing logger.error(formatedtxt)
   

    except Exception as e:
        logger.error(e)


if __name__ == '__main__':
    main()
