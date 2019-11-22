import pika
import sys
import json
import time
import string
import random
import time

# define globals
connectionString = 'localhost'
situation = []
stations = []
failedUnits = []
moveProperties = None
itemCode = ""

# The Id generator creates a new message id


def id_generator(size=15, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

# The loader function reads both item and config json files

def itemLoader():
    #loading item.json
    file = open("item.json", "r")
    RawItem = file.read()
    file.close()
    return RawItem

def configLoader():
    #loading config.json
    file = open("config.json", "r")
    RawConf = file.read()
    file.close()
    return RawConf
    

# The start function initiates the program.

def start():
    global channel
    global situation
    global stations
    global moveProperties

    # Read and parse the item.json
    rawItem = itemLoader()

    # Initiate situation  
    item = json.loads(rawItem)
    itemCodes = item["ItemCodes"]
    for item in itemCodes:
        situation.append(
                {"ItemCode": item['ItemCode'], "StationSequenceNumber": None})
        
        for key in item.keys():
            situation[-1][key] = item[key]

    # Initiate stations
    stationProperties = json.loads(rawConf)
    moveProperties = stationProperties
    properties = stationProperties["Stations"]
    # moveProperties.append(str(stationProperties["DriveThrough"]))
    for property in properties:
        stations.append(property)

    # Send first message
    nextStep()

# The forward function moves given unit (x) to a new station (nextSeqNbr).


def forward(x, nextSeqNbr):
    global channel
    global situation
    global stations
    global failedUnits
    global moveProperties
    global itemCode

    itemCode = x["ItemCode"]

    # get the desired station
    station = next(
        p for p in stations if p["StationSequenceNumber"] == nextSeqNbr)

    propBase = None
    if itemCode in failedUnits:
        propBase = moveProperties["DriveThrough"]
    else:
        propBase = station

    signalCode = propBase["SignalCode"]
    commandCode = propBase["CommandCode"]
    commandDescription = propBase["CommandDescription"]
    workflowVersionCode = propBase["WorkflowVersionCode"]
    responseSignalCode = propBase["ResponseSignalCode"]

    rawConf = configLoader()

    # get attributes
    attributes = json.loads(rawConf)

    proCode = attributes["ProcessCode"]
    workStationCode = attributes["WorkstationCode"]

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
    msgdtl["SignalBody"]["ItemCode"] = itemCode
    msgdtl["SignalBody"]["StationSequenceNumber"] = nextSeqNbr
    msgdtl["SignalBody"]["ResponseSignalCode"] = responseSignalCode
    msgdtl["SignalBody"]["ProcessCode"] = proCode
    msgdtl["ProcessCode"] = proCode
    msgdtl["WorkstationCode"] = workStationCode
    hdrs["WorkstationCode"] = workStationCode

    ts = time.time()
    msgdtl["UtcTimeStamp"] = ts

    for key in x.keys():
        if key in ("ItemCode", "StationSequenceNumber"):
            continue

        if "IsIdentification" not in station or station["IsIdentification"] == False:
            if key == "InfoString":
                continue

        if "IsScaling" not in station or station["IsScaling"] != True:
            if key.startswith("Measured") or key.startswith("Scaled"):
                continue

        msgdtl["SignalBody"][key] = x[key]

    if "IsKickOut" in station and station["IsKickOut"] == True:
        msgdtl["SignalBody"]["KickOutFlag"] = "True"
        

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

    # update the situation
    match = next((x for x in situation if x["ItemCode"] == itemCode))
    match["StationSequenceNumber"] = nextSeqNbr
    if "IsKickOut" in station and station["IsKickOut"] == True:
        situation.remove(match)

    print("Message sent, waiting for a response")

# The nextStep function gets called to make next move in the program.
def nextStep():
    global situation
    global stations
    global failedUnits

    # Print current situaton
    printSituation(situation)

    # find highest possible entry candidate to the wrapping line
    candidates = []
    for i in situation:
        if i["StationSequenceNumber"] is not None:
            candidates.insert(0, i)
        else:
            candidates.insert(0, i)
            break

    # find first candidate that can be moved forward
    for i in candidates:
        seqNbr = i["StationSequenceNumber"]

        nextSeqNbr = 0

        if seqNbr is None:
            nextSeqNbr = stations[0]["StationSequenceNumber"]
        else:
            station = next(
                (x for x in stations if x["StationSequenceNumber"] == seqNbr))
            index = stations.index(station)
            if index + 1 >= len(stations):
                continue

            nextSeqNbr = stations[index+1]["StationSequenceNumber"]

        nextStation = next(
            (x for x in stations if x["StationSequenceNumber"] == nextSeqNbr))

        isKickOut = False
        if "IsKickOut" in nextStation and nextStation["IsKickOut"] == True:
            isKickOut = True

        match = next(
            (x for x in candidates if x["StationSequenceNumber"] == nextSeqNbr), None)
        if match is None or isKickOut == True:
            forward(i, nextSeqNbr)
            return True
        

    return False

# Print details of current situation.
def printSituation(situation):
    print("------------------------- ")
    print("Current situation: ")
    print("Station\tUnit")
    for i in situation:
        print(str(i["StationSequenceNumber"]) + "\t" + i["ItemCode"])
    print("------------------------- ")
    print()

# Print details of sent message.
def printSend(msgdtl):
    print("  Sending Message:         ===> ")
    print("   |  SignalCode: " + msgdtl["SignalCode"])
    print("   |  ItemCode: " + msgdtl["SignalBody"]["ItemCode"])
    print("   |  StationSequenceNumber: " + str(msgdtl["SignalBody"]["StationSequenceNumber"]))

    for key in msgdtl["SignalBody"].keys():
        if key in ("ItemCode", "StationSequenceNumber", "ProcessCode", "ResponseSignalCode"):
            continue

        print("   |  " + key + ": " + str(msgdtl["SignalBody"][key]))

    print()

# Print details of received message.
def printReply(reply):
    print("   <===   Message received: ")
    print("          |  SignalCode: " + reply["SignalCode"])
    print("          |  ItemCode: " + reply["SignalData"]["ItemCode"])
    print("          |  StationSequenceNumber: " + str(reply["SignalData"]["StationSequenceNumber"]))
    print("          |  TransactionResult: " + str(reply["SignalData"]["TransactionResult"]))
    if("InfoString" in reply["SignalData"] and str(reply["SignalData"]["InfoString"]) != ""):
        print("          |  InfoString: " + str(reply["SignalData"]["InfoString"]))
    print()

# The callback function gets called when MQ message is received
def callback(ch, method, properties, body):
    global channel
    global situation
    global failedUnits
    global itemCode

    reply = json.loads(body)
    printReply(reply)

    rawData = json.loads(configLoader())
    sleepTime = rawData["SleepDelay"]

    print("Sleeping " + str(sleepTime) + " seconds...")

    time.sleep(sleepTime)

    if(reply["SignalData"]["ItemCode"] != itemCode):
        for item in situation:
            if (item["ItemCode"] == itemCode):
                item["ItemCode"] = reply["SignalData"]["ItemCode"]

    if(str(reply["SignalData"]["TransactionResult"]) == "False"):
        failedUnits.append(reply["SignalData"]["ItemCode"])
        # sys.exit()

    # Call nextStep to evaluate next move. If none, exit.
    if not nextStep():
        print("Work is done. Bye!")
        sys.exit()


# Start of initialization
print('TIPS-Wrapline-Tester (wr-tester.py)')
print('Version 2019.11.22')

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

if( result.method.message_count != 0):
    print("there are messages in queue ('wr-tester') , messages " + str(result.method.message_count))
    #exit()
    choice = input(print('Do you want to Empty the queue: Y/N'))
    print('Press Y for Yes or N for not, just use Capital letter')
    if choice =='Y':
        channel.queue_purge(queue='wr-tester')
        print("queue is now empty")

    elif  choice =='N':
        exit()
    else:
        print('GoodBye Let see soon')
        exit()

print('RabbitMQ setup complete')
print()

# Call start to send the first message
start()

# Start consume loop
channel.basic_consume(
    queue='wr-tester', on_message_callback=callback, auto_ack=True)

channel.start_consuming()
