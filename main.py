import pika
import random

from datetime import datetime


def time_gen():
    """
    A function that creates a random date between 03-09-2020 and
    03-09-2021 returning it in the str() type and format: "YYYY-MM-DD, HH:MM:SS"
    """
    timestamp = random.randint(1630683282, 1633275282)
    date_time = datetime.fromtimestamp(timestamp)
    return str(date_time.strftime("%Y-%m-%d|%H:%M:%S"))


def temperature_gen():
    """
    A function that creates a random temperature between 100 and
    373 degrees Kelvin that returns it as str()
    """
    temper_kel = random.randint(100, 373)
    return str(temper_kel)


def pressure_gen():
    """
    A function that creates a random pressure between 10 and 100 Pascal returning it in str() type
    """
    pressure_pa = random.randint(10, 100)
    return str(pressure_pa)


def sender(channel, message, connection, event):
    """
    In this function, we collect the task sent to the recipient and create it in the queue, with parameters that
    allow not to lose it if there is no active receiver
    """
    msg = message + ';' + event
    # Creating a complete message to send to the handler along with the event
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


for i in range(10):
    time, temperature, pressure = time_gen(), temperature_gen(), pressure_gen()
    # Assigning random values to time, temperature and pressure variables

    sensor = time + ';' + temperature + ';' + pressure
    # Creating a single status message from the sensor variables

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    # Creating a connection to the RabbitMQ task queue server

    if int(temperature) > 300:
        # Checking the Temperature Limit
        sender(channel, sensor, connection, event="Alert")
    else:
        sender(channel, sensor, connection, event="Log")
