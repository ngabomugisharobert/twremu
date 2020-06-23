import json
import pika
import pyodbc
from pyrabbit.api import Client


# ------------------------------------------------------------------
# ------------------clearing Rabbitmq (queues)----------------------
# ------------------------------------------------------------------


# loading config.json

def configLoader():
    file = open("config.json", "r")
    RawConf = file.read()
    file.close()
    return RawConf


# getting list of all queue

def getQueues():
    cl = Client('localhost:15672', 'guest', 'guest')
    return [q['name'] for q in cl.get_queues()]


# Start of initialization
print('Emptying all queue (empty_queue.py)')
print('Version 2020.06.22')

# reading configurations
rawConf = configLoader()
connectionProperties = json.loads(rawConf)
rabbitmq = connectionProperties["Rabbitmq"]

user = rabbitmq["User"]
password = rabbitmq["Password"]
host = rabbitmq["Host"]
port = rabbitmq["Port"]
virtualHost = rabbitmq["VirtualHost"]

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
    print("Delete")
    cursor = conn.cursor()
    cursor.execute(
        'delete from TIX_MQRECEIVERQUEUE'
    )
    conn.commit()

# connecting to the database


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
