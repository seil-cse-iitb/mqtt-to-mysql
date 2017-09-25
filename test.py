import paho.mqtt.client as mqtt
mqttHost='10.129.23.41'
mqttPort=1883
mqttKeepalive=60
mqttTopicName='nodemcu/kresit/dht/LH'
mqttc = mqtt.Client("LH Data -- Parth MTP")
mqttc.connect(mqttHost,mqttPort,mqttKeepalive)
def pub():

    msg="12,27.80,71.80,3.54" #your message here
    mqttc.publish(mqttTopicName,msg,2)
    print "msg sent "+msg

pub()
