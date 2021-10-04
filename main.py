import os
import pika
import random
import sys
from datetime import datetime


def time_gen():
    """
    A function that creates a random date between 03-09-2020 and
    03-09-2021 returning it in the str() type and format: "YYYY-MM-DD, HH:MM:SS"
    :return:
    """
    timestamp = random.randint(1630683282, 1633275282)
    date_time = datetime.fromtimestamp(timestamp)
    return str(date_time.strftime("%Y-%m-%d|%H:%M:%S"))


def temperature_gen():
    """
    A function that creates a random temperature between 100 and
    373 degrees Kelvin that returns it as str()
    :return:
    """
    temper_kel = random.randint(100, 373)
    return str(temper_kel)


def pressure_gen():
    """
    A function that creates a random pressure between 10 and 100 Pascal returning it in str() type
    :return:
    """
    pressure_pa = random.randint(10, 100)
    return str(pressure_pa)


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)


def sender(channel, message, connection, event):
    msg = message+';'+event
    channel.queue_declare(queue='SQLLog', durable=True)
    channel.basic_publish(
        exchange='',
        routing_key='SQLLog',
        body=msg,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    print(event, "Был отправлен")
    connection.close()


# def main():
for i in range(10):
    time, temperature, pressure = time_gen(), temperature_gen(), pressure_gen()

    sensor = time + ';' + temperature + ';' + pressure
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    if int(temperature) > 300:
        sender(channel, sensor, connection, event="Alert")
    else:
        sender(channel, sensor, connection, event="Log")

# if __name__ == '__main__':
#     try:
#         main()
#     except KeyboardInterrupt:
#         print('Interrupted')
#         try:
#             sys.exit(0)
#         except SystemExit:
#             os._exit(0)
