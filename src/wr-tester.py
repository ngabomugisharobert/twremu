import pika
import sys
import json
import time
import string
import random

# define globals
connectionString = 'localhost'
situation = []
stations = []

# The Id generator creates a new message id


def id_generator(size=15, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

# The start function initiates the program.


def start():
    global channel
    global situation
    global stations

    # Read and parse the item.json
    file = open("item.json", "r")
    rawItem = file.read()
    file.close()

    # Initiate situation
    item = json.loads(rawItem)
    itemCodes = item["ItemCodes"]
    for item in itemCodes:
        situation.append(
            {"ItemCode": item['ItemCode'], "StationSequenceNumber": None, "ScaledNetWeight": item["ScaledNetWeight"]})

    # Read and parse the config.json
    file = open("config.json", "r")
    rawConf = file.read()
    file.close()

    # Initiate stations
    stationProperties = json.loads(rawConf)
    properties = stationProperties["Stations"]
    for property in properties:
        stations.append(property)

    # Send first message
    nextStep()

# The forward function moves given unit (x) to a new station (nextSeqNbr).


def forward(x, nextSeqNbr):
    global channel
    global situation
    global stations

    itemCode = x["ItemCode"]
    scaledNetWeight = x["ScaledNetWeight"]

    # get the desired station
    station = next(
        p for p in stations if p["StationSequenceNumber"] == nextSeqNbr)
    signalCode = station["SignalCode"]
    commandCode = station["CommandCode"]
    commandDescription = station["CommandDescription"]
    workflowVersionCode = station["WorkflowVersionCode"]
    responseSignalCode = station["ResponseSignalCode"]

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

    if "IsScaling" in station and station["IsScaling"] == True:
        msgdtl["SignalBody"]["ScaledNetWeight"] = scaledNetWeight

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

    print('sending')
    channel.basic_publish(exchange='(TIX Hub)',
                          routing_key=key,
                          body=json.dumps(msgdtl),
                          properties=props,
                          mandatory=False)

    # update the situation
    match = next((x for x in situation if x["ItemCode"] == itemCode))
    match["StationSequenceNumber"] = nextSeqNbr
    if "IsKickOut" in station and station["IsKickOut"] == True:
        print("kickout!!")
        situation.remove(match)

# The nextStep function gets called to make next move in the program.


def nextStep():
    global situation
    global stations

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
        print("i: "+"itemCode: "+str(i["ItemCode"]) +
              ", StationSequenceNumber: "+str(i["StationSequenceNumber"]))
        seqNbr = i["StationSequenceNumber"]
        print("seqNbr: "+str(seqNbr))
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

        print("nextSeqNbr: "+str(nextSeqNbr))

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

# The callback function gets called when MQ message is received


def callback(ch, method, properties, body):
    global channel
    global situation

    print("callback")
    reply = json.loads(body)
    print("\n")
    print("the reply from emulator")
    print("*******************************************************************************************************")
    print(reply)
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("\n")

    if(reply["SignalData"]["TransactionResult"] != "True"):
        print("ERROR!!!")
        sys.exit()

    # Call nextStep to evaluate next move. If none, exit.
    if not nextStep():
        print("Work is done. Bye!")
        sys.exit()


# Start of initialization
print('Tips-Wrapline-Tester starting')
print('Connecting to RabbitMQ')

# Make a connection to MQ host
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=connectionString))
channel = connection.channel()

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

# Call start to send the first message
print("Sending the very first message")
start()

# Start consume loop
channel.basic_consume(
    queue='wr-tester', on_message_callback=callback, auto_ack=True)
channel.start_consuming()
