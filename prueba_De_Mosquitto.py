import paho.mqtt.client as mqtt

broker = "test.mosquitto.org"
port = 1883
topic = "prueba/mqtt"

# Callback cuando se conecta al broker
def on_connect(client, userdata, flags, rc):
    print("Conectado con código de resultado: " + str(rc))
    client.subscribe(topic)

# Callback cuando se recibe un mensaje
def on_message(client, userdata, msg):
    print(f"Mensaje recibido en {msg.topic}: {msg.payload.decode()}")

# Iniciar cliente MQTT
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker, port, 60)

# Publicar un mensaje de prueba
client.loop_start()
client.publish(topic, "Prueba de Mosquitto")
print("Mensaje publicado.")

# Mantener el script activo para recibir mensajes
try:
    while True:
        pass
except KeyboardInterrupt:
    print("Desconectando...")
    client.loop_stop()
    client.disconnect()
