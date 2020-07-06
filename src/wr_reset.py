import random
import string
import time
import json
import sys
import pika


# define globals
connectionString = 'localhost'


# The Id generator creates a new message id


def id_generator(size=15, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

# The loader function reads both item and config json files


def configLoader():

    if len(sys.argv) == 3 and str(sys.argv[1]) == "-config":
        # loading config.json
        file = open(str(sys.argv[2]), "r")
    else:
        # loading config.json
        file = open("config.json", "r")

    RawConf = file.read()
    file.close()
    return RawConf

def start():
    rawConf = configLoader()

    # get attributes
    attributes = json.loads(rawConf)

    signalCode  = attributes["Reset"]["SignalCode"]
    commandCode = attributes["Reset"]["CommandCode"]
    commandDescription = attributes["Reset"]["CommandDescription"]
    workflowVersionCode = attributes["Reset"]["WorkflowVersionCode"]
    responseSignalCode = attributes["Reset"]["ResponseSignalCode"]

    proCode = attributes["ProcessCode"]

    # read sample message file
    file = open("sample_message.json", "r")
    rawmsg = file.read()
    file.close()
    msg = json.loads(rawmsg)
    hdrs = msg["Header"]
    mqmsgid = msg["MsgId"]
    msgtype = msg["Type"]
    msgdtl = msg["Body"]

    # replace needed fields
    msgdtl["Command"]["CommandCode"] = commandCode
    msgdtl["Command"]["CommandDescription"] = commandDescription
    msgdtl["Command"]["WorkflowVersionCode"] = workflowVersionCode
    msgdtl["SignalBody"]["ResponseSignalCode"] = responseSignalCode
    msgdtl["SignalBody"]["ProcessCode"] = proCode
    msgdtl["ProcessCode"] = proCode

    ts = time.time()
    msgdtl["UtcTimeStamp"] = ts

    msgdtl["SignalCode"] = signalCode

    # process and send the message
    hdr = {}
    if "SenderApplicationCode" in hdrs:
        hdr["SenderApplicationCode"] = hdrs["SenderApplicationCode"]
    if "TransactionId" in hdrs:
        hdr["TransactionId"] = hdrs["TransactionId"]
    if "TixUserId" in hdrs:
        hdr["TixUserId"] = hdrs["TixUserId"]
    if "WorkstationCode" in hdrs:
        hdr["WorkstationCode"] = hdrs["WorkstationCode"]

    props = pika.spec.BasicProperties(headers=hdr,
                                      delivery_mode=2,
                                      correlation_id=mqmsgid,
                                      message_id=id_generator(),
                                      type=msgtype)

    key = msgtype.split(':')[0]
    key = key.replace('Tips.Base.Messages.', '')
    key = key.replace('Message', '')

    printSend(msgdtl)

    channel.basic_publish(exchange='(TIX Hub)',
                          routing_key=key,
                          body=json.dumps(msgdtl),
                          properties=props,
                          mandatory=False)

    sys.exit()


print("Message sent  for ")

# Print details of sent message.

def printSend(msgdtl):
    print("  Sending Message:         ===> ")
    print("   |  SignalCode: " + msgdtl["SignalCode"])
    print()

def callback(ch, method, properties, body):
    global channel

    reply = json.loads(body)
    print(reply)
    rawData = json.loads(configLoader())
    sleepTime = rawData["SleepDelay"]

    print("Sleeping " + str(sleepTime) + " seconds...")

    time.sleep(sleepTime)
    sys.exit()


# Start of initialization
print('TIPS-Wrapline-Tester (wr_reset.py)')
print('Version 2020.07.03')

# Make a connection to MQ host

rawConf = configLoader()

# Initiate connection
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
# connection = pika.BlockingConnection(
#     pika.ConnectionParameters(host=connectionString))

print('Host: ' + host)
print('Port: ' + str(port))
print('VirtualHost: ' + virtualHost)
print('User: ' + user)
print('Connecting to RabbitMQ...')

# Setup the MQ host
print('Declaring exchange "(TIX Hub)"')
channel.exchange_declare(exchange='(TIX Hub)',
                         exchange_type='direct', durable=True)

print('Declaring exchange "Base.ToIpc.ToIpc"')
channel.exchange_declare(exchange='Base.ToIpc.ToIpc',
                         exchange_type='fanout', durable=True)

print('Create binding "(TIX Hub)" -> "Base.ToIpc.ToIpc" (routing="Base.ToIpc.ToIpc")')
channel.exchange_bind(destination='Base.ToIpc.ToIpc',
                      source='(TIX Hub)',
                      routing_key='Base.ToIpc.ToIpc',
                      arguments=None)

print('Declaring receiver queue "wr-tester"')
result = channel.queue_declare(queue='wr-tester')

print('Creating binding "Base.ToIpc.ToIpc" -> "wr-tester"')
channel.queue_bind(exchange='Base.ToIpc.ToIpc', queue='wr-tester')

if(result.method.message_count != 0):
    print("there are messages in queue ('wr-tester') , messages " +
          str(result.method.message_count))
    # exit()
    choice = input(print('Do you want to Empty the queue: Y/N'))
    print('Press Y for Yes or N for not, just use Capital letter')
    if choice == 'Y':
        channel.queue_purge(queue='wr-tester')
        print("queue is now empty")

    elif choice == 'N':
        exit()
    else:
        print('GoodBye Let see soon')
        exit()

print('RabbitMQ setup complete')
print()

# Call start to send the first message
start()
