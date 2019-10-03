import pika
import sys
import json
import time
import queue
import string
import random

# define globals
connectionString = 'localhost'
situation = []
stations = []
drive = None

# The Id generator creates a new message id


def id_generator(size=15, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

# The start function initiates the program.


def start():

    global drive
    # Read and parse the config.json
    file = open("config.json", "r")
    rawConf = file.read()
    file.close()

    # Initiate situation
    stationProperties = json.loads(rawConf)
    drive = stationProperties
    properties = stationProperties["Stations"]
    for property in properties:
        stations.append(property)

# The reply function sends a reply message back to the tester.


def reply(signalCodeResponse, itemCode, seqNbr, result, scaledNetWeight):
    global channel

    # Read and parse the reply message
    file = open("sample_reply.json", "r")
    rawmsg = file.read()
    file.close()
    reply = json.loads(rawmsg)
    hdrs = reply["Header"]
    mqmsgid = reply["MsgId"]
    msgtype = reply["Type"]
    msgdtl = reply["Body"]

    # replace needed fields
    msgdtl["SignalCode"] = signalCodeResponse
    msgdtl["SignalData"]["TransactionResult"] = result
    msgdtl["SignalData"]["ItemCode"] = itemCode
    msgdtl["SignalData"]["StationSequenceNumber"] = seqNbr
    if scaledNetWeight is not None:
        msgdtl["SignalData"]["ScaledNetWeight"] = scaledNetWeight

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

    publish = json.dumps(msgdtl)

    channel.basic_publish(exchange='(TIX Hub)',
                          routing_key=key,
                          body=publish,
                          properties=props,
                          mandatory=False)

    print('Replied: '+msgdtl["SignalCode"]+', ItemCode: '+msgdtl["SignalData"]["ItemCode"]+', Station: '+str(
        msgdtl["SignalData"]["StationSequenceNumber"])+', TransactionResult: '+str(msgdtl["SignalData"]["TransactionResult"]))
    print("--------------------------------------------------")


# The error function prints error message and replies to the sender
def error(signalCodeResponse, itemCode, seqNbr, mes=""):
    if mes != "":
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! \n" +
              str(mes) + " \n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    elif mes == "":
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    reply(signalCodeResponse, itemCode, seqNbr, False, None)
    sys.exit()

# The businessRules function checks the incoming message fit to the overall situation


def businessRules(signalCode, signalCodeResponse, itemCode, sequenceNumber, situation, msg_received):
    # B U S I N E S S - R U L E S -  P E R F O R M E D
    ################################################################
    # check that signal code matches sequence number
    # check that message is not a duplicate, i.e. unit is already in location
    # check that ID is done to unit which is not in the queue
    # check that any other signal than ID is done to unit that is already in the queue
    # check that one unit cannot overtake another
    # check that exit has the xxx flag
    ###############################################################

    global sys
    global stations
    global drive

    # 1st rule checking
    ms = "the Station is already occupied"
    for a in situation:
        if a["StationSequenceNumber"] == sequenceNumber and a["StationSequenceNumber"] != 5:
            error(signalCodeResponse, itemCode, sequenceNumber, ms)

    # 2nd rule checking
    ms = " this item has signalCode that does not match the station, something is wrong here."
    signal = next((p["SignalCode"]
                   for p in stations if p["StationSequenceNumber"] == sequenceNumber))
    if signalCode == signal or signalCode == drive["DriveThrough"]["SignalCode"]:
        print(" ACCEPTED ", signalCode, " to STATION ", next(
            (p["StationSequenceNumber"] for p in stations if p["StationSequenceNumber"] == sequenceNumber)))
    else:
        error(signalCodeResponse, itemCode, sequenceNumber, ms)

    # 4th rule
    station = next(
        (i for i in stations if i["StationSequenceNumber"] == sequenceNumber), None)
    item = next((i for i in situation if i["ItemCode"] == itemCode), None)
    ms = "the ID station already has this unit Item : ", itemCode, " in a Queue"
    if "IsIdentification" in station.keys() and station["IsIdentification"] == True and item is not None:
        error(signalCodeResponse, itemCode, sequenceNumber, ms)
    # 5th rule
    ms = "this unit Item : ", itemCode, " is not in a Queue"
    if ("IsIdentification" not in station.keys() or station["IsIdentification"] == False) and item is None:
        error(signalCodeResponse, itemCode, sequenceNumber, ms)

    # 6th rule
    ms = "this item :", itemCode, " has reached to exit station"
    if "KickOutFlag" in msg_received["SignalBody"] and msg_received["SignalBody"]["KickOutFlag"] == 'True':
        print(ms)

# The callback function gets called when MQ message is received


def callback(ch, method, properties, body):
    global channel
    global situation
    rslt = True

    # process incoming message
    msg = json.loads(body)
    signalCodeResponse = msg["SignalBody"]["ResponseSignalCode"]
    signalCode = msg["SignalCode"]
    itemCode = msg["SignalBody"]["ItemCode"]
    seqNbr = msg["SignalBody"]["StationSequenceNumber"]

    scaledNetWeight = 0
    if "ScaledNetWeight" in msg["SignalBody"]:
        scaledNetWeight = msg["SignalBody"]["ScaledNetWeight"]

    kickOutFlag = "False"
    if "KickOutFlag" in msg["SignalBody"]:
        kickOutFlag = msg["SignalBody"]["KickOutFlag"]

    if "ScaledNetWeight" not in msg["SignalBody"]:
        print('Received Message: '+signalCode+', ItemCode: ' +
              itemCode+', Station: '+str(seqNbr))
    else:
        print('Received Message: '+signalCode+', ItemCode: '+itemCode +
              ', Station: '+str(seqNbr) + ', ScalesNetWeight: ' + str(scaledNetWeight))

    #	call rules fnct to check if the incoming msg make sense SSN 1st msg from ID , 1 unit in 1 station, unit can't overtake another,
    #	write fake tester script that will send the message with error and add a reply message containing the transaction code = to fals
    print("--------------------------------------------------------------- \n")
    businessRules(signalCode, signalCodeResponse,
                  itemCode, seqNbr, situation, msg)
    print("--------------------------------------------------------------- \n")

    match = next((x for x in situation if x["ItemCode"] == itemCode), None)
    if match != None:
        situation.remove(match)

    if "ScaledNetWeight" in msg["SignalBody"]:
        situation.append(
            {"ItemCode": itemCode, "StationSequenceNumber": seqNbr, "ScaledNetWeight": scaledNetWeight})
    else:
        situation.append(
            {"ItemCode": itemCode, "StationSequenceNumber": seqNbr})

    if kickOutFlag == "True":
        print("kickout!!")
        matchByStation = next(
            (x for x in situation if x["StationSequenceNumber"] == seqNbr and x["ItemCode"] != itemCode), None)
        if matchByStation is not None:
            situation.remove(matchByStation)

    def mySort(x):
        return x["StationSequenceNumber"]

    situation.sort(key=mySort)

    output = json.dumps(situation, indent=4, sort_keys=True)

    print(output)
    if int(scaledNetWeight) >= 200:
        rslt = False

    time.sleep(2)
    print(rslt)
    reply(signalCodeResponse, itemCode, seqNbr, rslt, scaledNetWeight)


# Start of initialization
print('Tips-Wrapline-Emulator starting')
print('Connecting to RabbitMQ')

# Make a connection to MQ host
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=connectionString))
channel = connection.channel()

# Setup the MQ host
print('Declaring exchange "(TIX Hub)"')
channel.exchange_declare(exchange='(TIX Hub)',
                         exchange_type='direct', durable=True)

print('Declaring exchange "Base.FromIpc.IpcToPts"')
channel.exchange_declare(exchange='Base.FromIpc.IpcToPts',
                         exchange_type='fanout', durable=True)

print('Create binding "(TIX Hub)" -> "Base.FromIpc.IpcToPts" (routing="Base.FromIpc.IpcToPts")')
channel.exchange_bind(destination='Base.FromIpc.IpcToPts',
                      source='(TIX Hub)',
                      routing_key='Base.FromIpc.IpcToPts',
                      arguments=None)

print('Declaring receiver queue "tips-emulator"')
result = channel.queue_declare(queue='tips-emulator')

print('Creating binding "Base.FromIpc.IpcToPts" -> "tips-emulator"')
channel.queue_bind(exchange='Base.FromIpc.IpcToPts', queue='tips-emulator')

if( result.method.message_count != 0):
    print("there are messages in queue ('tips-emulator') , messages " + str(result.method.message_count))
    exit()

# Call start to set up globals
print("Starting program")
start()

print('Receiver starting')
print(' [*] Waiting for messages. To exit press CTRL+C')

# Start consume loop
channel.basic_consume(
    queue='tips-emulator', on_message_callback=callback, auto_ack=True)

channel.start_consuming()
