import pika
import sys
import json
import time

# define globals
mq_connect='localhost'
situation=[]

def start():
	global channel
	global situation

	# Read and parse the item.json
	file = open("item.json", "r")
	rawItem = file.read()
	file.close()

	# Initiate situation
	item = json.loads(rawItem)
	itemCodes = item["ItemCodes"]
	for item in itemCodes:
		situation.append( { "ItemCode": item['ItemCode'], "StationSequenceNumber": None, "ScaledNetWeight": item["ScaledNetWeight"] } )

	# Send first message
	nextStep()

def forward(x, nextSeqNbr):
	global channel
	global situation


	# Read and parse the item.json
	file2 = open("config.json", "r")
	rawConf = file2.read()
	file2.close()

	# Initiate situation
	stationProperties = json.loads(rawConf)


	signalCode=""
	itemCode=x["ItemCode"]
	scaledNetWeight = x["ScaledNetWeight"]
	kickOutFlag=False
	commandCode=""
	commandDescription=""
	workflowVersionCode=""
	responseSignalCode=""

	if nextSeqNbr==1:
		stationProperties["Station"]["StationSequenceNumber"]=1
		stationProperties["Station"]["StationName"]="identification"
		stationProperties["Station"]["StationCode"]="RWR2_ID"
		stationProperties["Station"]["IsScaling"]=False
		stationProperties["Station"]["IsKickOut"]=False
		signalCode="RWR2_ID"
		commandCode="WRAPLINE_IDENTIFY_W"
		commandDescription="Wrapline identification of unit"
		workflowVersionCode="WRAPLINE_IDENTIFY"
		responseSignalCode="RWR2_ID_RSP"
	elif nextSeqNbr==2:
		stationProperties["Station"]["StationSequenceNumber"]=2
		stationProperties["Station"]["StationName"]="measurement"
		stationProperties["Station"]["StationCode"]="RWR2_ME"
		stationProperties["Station"]["IsScaling"]=True
		stationProperties["Station"]["IsKickOut"]=False
		signalCode="RWR2_ME"
		commandCode="WRAPLINE_MEASURE_W"
		commandDescription="Wrapline measure of unit"
		workflowVersionCode="WRAPLINE_MEASURE"
		responseSignalCode="RWR2_ME_RSP"
	elif nextSeqNbr==3:
		stationProperties["Station"]["StationSequenceNumber"]=3
		stationProperties["Station"]["StationName"]="wrapping"
		stationProperties["Station"]["StationCode"]="RWR2_WR"
		stationProperties["Station"]["IsScaling"]=False
		stationProperties["Station"]["IsKickOut"]=False
		signalCode="RWR2_WR"
		commandCode="WRAPLINE_WRAP_W"
		commandDescription="Wrapline wrap of unit"
		workflowVersionCode="WRAPLINE_WRAP"
		responseSignalCode="RWR2_WR_RSP"
	elif nextSeqNbr==4:
		stationProperties["Station"]["StationSequenceNumber"]=4
		stationProperties["Station"]["StationName"]="Exit"
		stationProperties["Station"]["StationCode"]="RWR2_MO"
		stationProperties["Station"]["IsScaling"]=False
		stationProperties["Station"]["IsKickOut"]=False
		signalCode="RWR2_MO"
		commandCode="WRAPLINE_MOVE_W"
		commandDescription="Wrapline label of unit"
		workflowVersionCode="WRAPLINE_MOVE"
		responseSignalCode="RWR2_MO_RSP"
	elif nextSeqNbr==5:
		stationProperties["Station"]["StationSequenceNumber"]=5
		stationProperties["Station"]["StationName"]="Exit"
		stationProperties["Station"]["StationCode"]="RWR2_MO"
		stationProperties["Station"]["IsScaling"]=False
		stationProperties["Station"]["IsKickOut"]=True
		signalCode="RWR2_MO"
		commandCode="WRAPLINE_MOVE_W"
		commandDescription="Wrapline exit (move) of unit"
		workflowVersionCode="WRAPLINE_MOVE"
		responseSignalCode="RWR2_MO_RSP"
		kickOutFlag=True
	else:
		print("nextSeqNbr invalid value")
		return

	# read file
	file = open("sample_message.json","r")
	rawmsg=file.read()
	file.close()
	msg=json.loads(rawmsg)
	hdrs=msg["Header"]
	mqmsgid=msg["MsgId"]
	msgtype=msg["Type"]
	msgdtl=msg["Body"]

	# replace needed fields
	msgdtl["Command"]["CommandCode"]=commandCode
	msgdtl["Command"]["CommandDescription"]=commandDescription
	msgdtl["Command"]["WorkflowVersionCode"]=workflowVersionCode
	msgdtl["SignalBody"]["ItemCode"]=itemCode
	msgdtl["SignalBody"]["StationSequenceNumber"]=nextSeqNbr
	msgdtl["SignalBody"]["ResponseSignalCode"]=responseSignalCode
	if nextSeqNbr == 2:msgdtl["SignalBody"]["ScaledNetWeight"]=scaledNetWeight
	if kickOutFlag==True:
		msgdtl["SignalBody"]["KickOutFlag"]="True"

	msgdtl["SignalCode"]=signalCode

	hdr={}
	if "SenderApplicationCode" in hdrs:
		hdr["SenderApplicationCode"]=hdrs["SenderApplicationCode"]
	if "TransactionId" in hdrs:
		hdr["TransactionId"]=hdrs["TransactionId"]
	if "TixUserId" in hdrs:
		hdr["TixUserId"]=hdrs["TixUserId"]
	if "WorkstationCode" in hdrs:
		hdr["WorkstationCode"]=hdrs["WorkstationCode"]

	props = pika.spec.BasicProperties(headers=hdr,
		delivery_mode=2,
		correlation_id=mqmsgid,
		message_id=mqmsgid,
		type=msgtype)

	key = msgtype.split(':')[0]
	key = key.replace('Tips.Base.Messages.', '')
	key = key.replace('Message', '')

	msgdtl.update(stationProperties)

	print('sending')
	channel.basic_publish(exchange='(TIX Hub)',
                      routing_key=key,
                      body=json.dumps(msgdtl),
		properties=props,
		mandatory=False)

	# update the situation
	match = next((x for x in situation if x["ItemCode"]==itemCode))
	match["StationSequenceNumber"]=nextSeqNbr
	if kickOutFlag==True:
		print("kickout!!")
		situation.remove(match)
	return None

def nextStep():
	global situation

	# find highest possible entry candidate to the wrapping line
	candidates=[]
	for i in situation:
		if i["StationSequenceNumber"] is not None:
			candidates.insert(0, i)
		else:
			candidates.insert(0, i)
			break

	# find first candidate that can be moved forward
	for i in candidates:
		print("i: "+"itemCode: "+str(i["ItemCode"])+", StationSequenceNumber: "+str(i["StationSequenceNumber"]))
		seqNbr=i["StationSequenceNumber"]
		print("seqNbr: "+str(seqNbr))

		if seqNbr is None:
			seqNbr=0
		nextSeqNbr=seqNbr+1
		print("nextSeqNbr: "+str(nextSeqNbr))

		match = next((x for x in candidates if x["StationSequenceNumber"]==nextSeqNbr), None)
		if match is None:
			forward(i, nextSeqNbr)
			return True
	return False


def callback(ch, method, properties, body):
	global channel
	global situation

	print("callback")
	reply=json.loads(body)
	print("\n")
	print("the reply from emulator")
	print("*******************************************************************************************************")
	print(reply)
	print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

	print("\n")
	signalCode=reply["SignalCode"]
	itemCode=reply["SignalData"]["ItemCode"]
	seqNbr=reply["SignalData"]["StationSequenceNumber"]
# exit checking
	if not nextStep():
		print("Work is done. Bye!")
		sys.exit()

# Start of initialization
print('Tips-Wrapline-Tester starting')
print('Connecting to RabbitMQ')

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=mq_connect))
channel = connection.channel()

print('Declaring exchange "(TIX Hub)"')
channel.exchange_declare(exchange='(TIX Hub)', exchange_type='direct', durable=True)

print('Declaring exchange "Base.ToIpc.ToIpc"')
channel.exchange_declare(exchange='Base.ToIpc.ToIpc', exchange_type='fanout', durable=True)

print('Create binding "(TIX Hub)" -> "Base.ToIpc.ToIpc" (routing="Base.ToIpc.ToIpc")')
channel.exchange_bind(destination='Base.ToIpc.ToIpc',
	source='(TIX Hub)',
	routing_key='Base.ToIpc.ToIpc',
	arguments=None)

print('Declaring receiver queue "wr-tester"')
result = channel.queue_declare(queue='wr-tester')

print('Creating binding "Base.ToIpc.ToIpc" -> "wr-tester"')
channel.queue_bind(exchange='Base.ToIpc.ToIpc', queue='wr-tester')

print("Sending the very first message")
start()

channel.basic_consume(
    queue='wr-tester', on_message_callback=callback, auto_ack=True)
channel.start_consuming()
