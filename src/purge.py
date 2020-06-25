import json
import pika
import sys
import pyodbc
from pyrabbit.api import Client


# ------------------------------------------------------------------
# ------------------clearing Rabbitmq (queues)----------------------
# ------------------------------------------------------------------


# getting list of all queue

def getQueues():
    cl = Client('localhost:15672', 'guest', 'guest')
    return [q['name'] for q in cl.get_queues()]


# Start of initialization
print('Emptying all queue (empty_queue.py)')
print('Version 2020.06.22')


user = "guest"
password = "guest"
host = "localhost"
port = 5672
virtualHost = "/"

credentials = pika.PlainCredentials(user, password)
parameters = pika.ConnectionParameters(host,
                                       port,
                                       virtualHost,
                                       credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# emptying all queues
queues = getQueues()
for queue in queues:
    channel.queue_purge(queue)


print('RabbitMQ setup clearing completed')
print()


# ------------------------------------------------------------------
# ------------------clearing sql (tixmqreceiver)--------------------
# ------------------------------------------------------------------


# emptying table TIX_MQRECEIVERQUEUE

def delete(conn):
    print("Deleting data from TIX_MQRECEIVERQUEUE ")
    cursor = conn.cursor()
    cursor.execute(
        'delete from TIX_MQRECEIVERQUEUE'
    )
    conn.commit()


# connecting to the database
if len(sys.argv) == 2:
    db = str(sys.argv[1])
    conn = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        "Server=(localdb)\MSSQLLocalDB;"
        "Database="+db+";"
        "Trusted_Connection=yes;"
    )
else:
    conn = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        "Server=(localdb)\MSSQLLocalDB;"
        "Database=tipsdev;"
        "Trusted_Connection=yes;"
    )


delete(conn)
conn.close()

print('Environment is clean')
print()
