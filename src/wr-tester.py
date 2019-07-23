import pika
import sys
import json

# define globals
mq_connect='localhost'
situation=[]

def start():
	global channel
	global situation

	# Read and parse the config.json
	file = open("config.json", "r")
	rawConfig = file.read()
	file.close()

	# Initiate situation
	config = json.loads(rawConfig)
	itemCodes=config["ItemCodes"]
	for itemCode in itemCodes:
		situation.append( { "ItemCode": itemCode, "StationSequenceNumber": None } )

	# Send first message
	nextStep()

def forward(x, nextSeqNbr):
	global channel
	global situation

	signalCode=""
	itemCode=x["ItemCode"]
	kickOutFlag=False

	if nextSeqNbr==1:
		signalCode="RWR2_ID"
	elif nextSeqNbr==2:
		signalCode="RWR2_ME"
	elif nextSeqNbr==3:
		signalCode="RWR2_WR"
	elif nextSeqNbr==4:
		signalCode="RWR2_MO"
	elif nextSeqNbr==5:
		signalCode="RWR2_MO"
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
	msgdtl["Command"]["CommandCode"]="TEST"
	msgdtl["Command"]["CommandDescription"]="TEST2"
	msgdtl["Command"]["WorkflowVersionCode"]="TEST3"
	msgdtl["SignalBody"]["ItemCode"]=itemCode
	msgdtl["SignalBody"]["StationSequenceNumber"]=nextSeqNbr
	msgdtl["SignalBody"]["ResponseSignalCode"]="TEST4"
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
		print("i: "+str(i))
		seqNbr=i["StationSequenceNumber"]
		print("seqNbr: "+str(seqNbr))

		if seqNbr is None:
			seqNbr=0
		nextSeqNbr=seqNbr+1
		print("nextSeqNbr: "+str(nextSeqNbr))

		match = next((x for x in candidates if x["StationSequenceNumber"]==nextSeqNbr), None)
		if match is None:
			forward(i, nextSeqNbr)
			break

def callback(ch, method, properties, body):
	global channel
	global situation

	print("callback")
	reply=json.loads(body)
	print("\n")
	print(reply)
	print("\n")
	signalCode=reply["SignalCode"]
	itemCode=reply["SignalBody"]["ItemCode"]
	seqNbr=reply["SignalBody"]["StationSequenceNumber"]

	# TODO Checks

	nextStep()

	# check for exit
	if not situation:
		sys.exit()

# Start of initialization
print('Tips-Wrapline-Tester starting')
print('Connecting to RabbitMQ')

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=mq_connect))
channel = connection.channel()

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
