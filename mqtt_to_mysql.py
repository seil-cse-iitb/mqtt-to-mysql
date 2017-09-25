import paho.mqtt.client as mqtt
import paho.mqtt
import time
from datetime import datetime
from MySQLdb import *
import sys
from db import insert_to_db

MQTT_HOST="10.129.23.41"
MQTT_PORT=1883
MQTT_TOPIC="nodemcu/kresit/dht/"
room_name=""
def get_mqtt_msg():

    def on_connect(client, userdata, flags, rc):
        client.subscribe(MQTT_TOPIC+room_name)
        print "Subscibed to "+MQTT_TOPIC+room_name

    def on_message(client, userdata, msg):
      print datetime.now().strftime('%H:%M:%S  : ')+ "Recived "+  msg.payload
      message=msg.payload.split(",")
      node_id=int(message[0])
      temp=float(message[1])
      humidity=float(message[2])
      volt=float(message[3])
      ts=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
      insert_to_db(room_name,ts,node_id,temp,humidity,volt)

    client = mqtt.Client("LH Data -- Parth")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()



if __name__=="__main__":
    room_name=sys.argv[1]
    get_mqtt_msg()
