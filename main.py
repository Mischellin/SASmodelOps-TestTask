import random
from datetime import datetime
import pika


def time_gen():
    timestamp = random.randint(1630683282,1633275282)
    date_time = datetime.fromtimestamp(timestamp)
    return date_time.strftime("%Y-%m-%d, %H:%M:%S")


def temperature_gen():
    temper_kel = random.randint(100, 373)
    return temper_kel


def pressure_gen():
    pressure_pa = random.randint(10, 100)
    return pressure_pa
def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)


time, temperature, pressure = time_gen(), temperature_gen(), pressure_gen()
sensor = [time, temperature, pressure]
print(sensor)
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')
# channel.basic_publish(exchange='',
#                       routing_key='hello',
#                       body='Hello World!')
# print(" [x] Sent 'Hello World!'")
# connection.close()