import pika
import sys
import json
import time
import queue
import string
import random

mq_connect='localhost'

situation=[]
Stations=[]
print('Tips-Wrapline-Emulator starting')
print('Connecting to RabbitMQ')

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=mq_connect))
channel = connection.channel()

print('Declaring exchange "(TIX Hub)"')
channel.exchange_declare(exchange='(TIX Hub)', exchange_type='direct', durable=True)

print('Declaring exchange "Base.FromIpc.IpcToPts"')
channel.exchange_declare(exchange='Base.FromIpc.IpcToPts', exchange_type='fanout', durable=True)

print('Create binding "(TIX Hub)" -> "Base.FromIpc.IpcToPts" (routing="Base.FromIpc.IpcToPts")')
channel.exchange_bind(destination='Base.FromIpc.IpcToPts',
	source='(TIX Hub)',
	routing_key='Base.FromIpc.IpcToPts',
	arguments=None)

print('Declaring receiver queue "tips-emulator"')
result = channel.queue_declare(queue='tips-emulator')

print('Creating binding "Base.FromIpc.IpcToPts" -> "tips-emulator"')
channel.queue_bind(exchange='Base.FromIpc.IpcToPts', queue='tips-emulator')

print('Defining business rules function')

def id_generator(size=15, chars=string.ascii_letters + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))


# Read and parse the config.json
file = open("config.json", "r")
rawConf = file.read()
file.close()

# Initiate situation
stationProperties = json.loads(rawConf)
properties = stationProperties["Stations"]
for property in properties:
	Stations.append(property)


def init_msg(itemCode,seqNbr,result,scaledNetWeight=""):

	file = open("sample_reply.json", "r")
	rawmsg = file.read()
	file.close()
	reply = json.loads(rawmsg)
	hdrs=reply["Header"]
	mqmsgid=reply["MsgId"]
	msgtype=reply["Type"]
	msgdtl=reply["Body"]

	msgdtl["SignalCode"]
	msgdtl["SignalData"]["TransactionResult"] = result
	msgdtl["SignalData"]["ItemCode"] = itemCode
	msgdtl["SignalData"]["StationSequenceNumber"] = seqNbr
	if seqNbr == 2:

        	msgdtl["SignalData"]["ScaledNetWeight"] = scaledNetWeight
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
		message_id= id_generator(),
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

	print('Replied: '+msgdtl["SignalCode"]+', ItemCode: '+msgdtl["SignalData"]["ItemCode"]+', Station: '+str(msgdtl["SignalData"]["StationSequenceNumber"])+', TransactionResult: '+str(msgdtl["SignalData"]["TransactionResult"]))
	print("--------------------------------------------------")


def error_msg(itemCode,seqNbr,mes=""):

	if mes != "":
    		print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! \n"+ str(mes) + " \n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
	elif mes == "":
			print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

	init_msg(itemCode,seqNbr,False)
	sys.exit()



def business_rules(signalCode, itemCode, sequenceNumber,situation,msg_received):
	global sys
	global Stations

	match = next((x for x in situation if x["ItemCode"]==itemCode), None)
	match2 = next((y for y in situation if y["StationSequenceNumber"] == sequenceNumber), None)

#1st rule checking
	ms = "the Station is already occupied"
	for a in situation:
    		if a["StationSequenceNumber"]== sequenceNumber and a["StationSequenceNumber"]!= 5:
    				error_msg(itemCode,sequenceNumber,ms)


#2nd rule checking

	ms = " this item has signalCode that does not match the station, something is wrong here."
	if signalCode == next((p["SignalCode"] for p in Stations if p["StationSequenceNumber"] == sequenceNumber)) and sequenceNumber == 1:
    		print(" ACCEPTED ",signalCode," to STATION ID")
	elif signalCode == next((p["SignalCode"] for p in Stations if p["StationSequenceNumber"] == sequenceNumber)) and sequenceNumber == 2:
    		print(" ACCEPTED ",signalCode," to STATION ME")
	elif signalCode == next((p["SignalCode"] for p in Stations if p["StationSequenceNumber"] == sequenceNumber)) and sequenceNumber == 3:
    		print(" ACCEPTED ",signalCode," to STATION WR")
	elif signalCode == next((p["SignalCode"] for p in Stations if p["StationSequenceNumber"] == sequenceNumber)) and sequenceNumber == 4:
    		print(" ACCEPTED ",signalCode," to STATION MO")
	elif signalCode == next((p["SignalCode"] for p in Stations if p["StationSequenceNumber"] == sequenceNumber)) and sequenceNumber == 5:
    		print(" ACCEPTED ",signalCode," to STATION MO")
	else:
    		error_msg(itemCode,sequenceNumber,ms)

#4th rule
	ms ="the ID station already has this unit Item : ",itemCode," in a Queue"
	if signalCode == next((p["SignalCode"] for p in Stations if p["StationSequenceNumber"] == 1)) and itemCode in situation:
			error_msg(itemCode,sequenceNumber,ms)

#5th rule

	ms ="this unit Item : ",itemCode," is not in a Queue"
	if signalCode != next((p["SignalCode"] for p in Stations if p["StationSequenceNumber"] == 1)) and not any(itemCode for d in situation):
			error_msg(itemCode,sequenceNumber,ms)

#6th rule

	ms= "this item :", itemCode , " has reached to exit station"
	if "KickOutFlag" in msg_received["SignalBody"] and msg_received["SignalBody"]["KickOutFlag"] == 'True' :
			print(ms)


	###  B U S I N E S S - R U L E S -  P E R F O R M E D
	################################################################
	######### check that signal code matches sequence number
	######### check that message is not a duplicate, i.e. unit is already in location
	######### check that ID is done to unit which is not in the queue
	######### check that any other signal than ID is done to unit that is already in the queue
	######### check that one unit cannot overtake another
	######### check that exit has the xxx flag
	###############################################################

print('Defining callback function')

def callback(ch, method, properties, body):
	global channel
	global situation
	msg=json.loads(body)
	ms = "malformed message. No SignalBody"
	if "SignalBody" not in msg:return
	signalCode=msg["SignalCode"]
	itemCode=msg["SignalBody"]["ItemCode"]
	seqNbr=msg["SignalBody"]["StationSequenceNumber"]
	if  "ScaledNetWeight" in msg["SignalBody"]:scaledNetWeight=msg["SignalBody"]["ScaledNetWeight"]
	kickOutFlag="False"
	if  "KickOutFlag" in msg["SignalBody"]:
		kickOutFlag=msg["SignalBody"]["KickOutFlag"]

	if "ScaledNetWeight" not in msg["SignalBody"]:
		print('Received Message: '+signalCode+', ItemCode: '+itemCode+', Station: '+str(seqNbr))
	else:
		print('Received Message: '+signalCode+', ItemCode: '+itemCode+', Station: '+str(seqNbr)+ ', ScalesNetWeight: ' + str(scaledNetWeight))
#	call rules fnct to check if the incoming msg make sense SSN 1st msg from ID , 1 unit in 1 station, unit can't overtake another,
#	write fake tester script that will send the message with error and add a reply message containing the transaction code = to fals

	print("--------------------------------------------------------------- \n")
	business_rules(signalCode,itemCode,seqNbr,situation,msg)

	print("--------------------------------------------------------------- \n")


	match = next((x for x in situation if x["ItemCode"]==itemCode), None)
	if match != None:
		situation.remove(match)
	if seqNbr == 2:
		situation.append( { "ItemCode": itemCode, "StationSequenceNumber": seqNbr, "ScaledNetWeight" : scaledNetWeight } )
	else:
		situation.append( { "ItemCode": itemCode, "StationSequenceNumber": seqNbr} )

	if kickOutFlag=="True":
		print("kickout!!")
		matchByStation = next((x for x in situation if x["StationSequenceNumber"]==seqNbr and x["ItemCode"]!=itemCode), None)
		if matchByStation is not None:
			situation.remove(matchByStation)

	def mySort(x):
		return x["StationSequenceNumber"]

	situation.sort(key=mySort)

	output=json.dumps(situation, indent=4, sort_keys=True)

	print(output)
	print()


	time.sleep(2)
	if seqNbr == 2:
		init_msg(itemCode, seqNbr, True,scaledNetWeight)
	else:
		init_msg(itemCode,seqNbr,True)
print('Receiver starting')
print(' [*] Waiting for messages. To exit press CTRL+C')

channel.basic_consume(
    queue='tips-emulator', on_message_callback=callback, auto_ack=True)

channel.start_consuming()
