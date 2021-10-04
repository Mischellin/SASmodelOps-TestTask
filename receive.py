import datetime
import os
import pika
import psycopg2
import sys
import datetime
from psycopg2.extras import DictCursor
from contextlib import closing
from wpas import *
from psycopg2 import sql


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='SQLLog', durable=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        #
        row = body.decode().split(';')
        # date = row[0].split('|')
        # date = datetime.time

        print(row)
        with closing(
                psycopg2.connect(dbname='SASTest', user='postgres', password=passwordDB, host='localhost')) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                conn.autocommit = True
                values = [(row[0], row[1], row[2])]
                if row[3] != "Alert":
                    insert = sql.SQL('INSERT INTO logger.log ("date", "temperature", "preassure") VALUES {}').format(
                        sql.SQL(',').join(map(sql.Literal, values))
                    )
                elif row[3] == "Log":
                    insert = sql.SQL('INSERT INTO log (Date, Temperature, Pressure) VALUES {}').format(
                        sql.SQL(',').join(map(sql.Literal, values))
                    )
                cursor.execute(insert)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='SQLLog', on_message_callback=callback)
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
