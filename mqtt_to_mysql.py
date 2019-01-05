import paho.mqtt.client as mqtt
import paho.mqtt
import logging
from logging.config import fileConfig
import time
from datetime import datetime
from MySQLdb import *
import sys
from config import *





def insert_to_mysql(ts, sensor_id, temperature, humidity, battery):
        try:
            con = connect(IP, USERNAME, PASSWORD, DB)
        except:
            logging.error("Execution failed -- Rollback in progress..")

        cur = con.cursor()

        sql  = "insert into temp_data(time_stamp, sensor_id, temp, humidity, battery) values('%f','%s','%f','%f','%f')" %(ts, sensor_id, temperature, humidity, battery)

        try:
                cur.execute(sql)
                con.commit()
        except Exception as e:
                logging.error("Insertion failed with error "+str(e))
                con.rollback()
                logging.info("Failed Data: "+str(ts)+","+ str(sensor_id)+","+ str(temperature)+","+ str(humidity)+","+ str(battery))
        con.close()




def start_logging():

    def on_connect(client, userdata, flags, rc):
        client.subscribe(MQTT_TOPIC)
        logger.info("Subscibed to "+MQTT_TOPIC)

    def on_message(client, userdata, msg):
        try:
            message=msg.payload.split(",")
            sensor_id=message[0]
            temp=float(message[1])
            humidity=float(message[2])
            volt=float(message[3])
            ts=int(time.time())
        except Exception as e:
            logging.error("Data typecaset with error :"+str(e))
            logging.info("Failed Data: "+msg)
        insert_to_mysql(ts, sensor_id, temp, humidity, volt)

    client = mqtt.Client("Temperature Collection -- Hareesh")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()



if __name__=="__main__":
    fileConfig('config.ini')
    logger=logging.getLogger()
    logger.info('Starting Temperature Logging')

    start_logging()
