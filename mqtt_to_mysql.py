import paho.mqtt.client as mqtt
import paho.mqtt
import logging
import time
from datetime import datetime
from MySQLdb import *
import sys
from db import insert_to_db
from config import *





def insert_to_mysql(room_name,ts,node_id,temp,humidity,volt):
        con = connect("10.129.23.161","writer","datapool","cooling")
        cur = con.cursor()

        sql  = "insert into temperature_analysis(room_name,timestamp,node_id,temperature,humidity,voltage)  values('%s','%s','%d','%f','%f','%f')" %(room_name,ts,node_id,temp,humidity,volt)
        # print sql
        try:
                # print "Executing sql"
                cur.execute(sql)
                # print "Executed sql"
                con.commit()
                # print "Insertion done -- Done"
        except:
                con.rollback()
                print "Execution failed -- Rollback in progress.."
        con.close()




def get_mqtt_msg():

    def on_connect(client, userdata, flags, rc):
        client.subscribe(MQTT_TOPIC)
        print "Subscibed to "+MQTT_TOPIC

    def on_message(client, userdata, msg):
      print datetime.now().strftime('%H:%M:%S  : ')+ "Recived "+  msg.payload
      message=msg.payload.split(",")
      node_id=int(message[0])
      temp=float(message[1])
      humidity=float(message[2])
      volt=float(message[3])
      ts=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
      print(ts,node_id,temp,humidity,volt)

    client = mqtt.Client("Temperature Variance")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.loop_forever()



if __name__=="__main__":
    get_mqtt_msg()
