import pika
import sys
import json
import time
import os
import queue

mq_connect='localhost'

situation=[]

print('Tips-Wrapline-Emulator starting')
print('Connecting to RabbitMQ')

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=mq_connect))
channel = connection.channel()

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


def error_msg(itemCode,seqNbr):
    	
		file = open("sample_reply.json", "r")
		rawmsg = file.read()
		file.close()
		reply = json.loads(rawmsg)

		hdrs=reply["Header"]
		mqmsgid=reply["MsgId"]
		msgtype=reply["Type"]
		msgdtl=reply["Body"]

		msgdtl["SignalCode"]
		msgdtl["SignalData"]["TransactionResult"]="False"
		msgdtl["SignalData"]["ItemCode"] = itemCode
		msgdtl["SignalData"]["StationSequenceNumber"] = seqNbr
		print("u can not skip any station or overTake other units in process")
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

		channel.basic_publish(exchange='(TIX Hub)',
						routing_key=key,
						body=json.dumps(msgdtl),
			properties=props,
			mandatory=False)

		print('Replied: '+msgdtl["SignalCode"]+', ItemCode: '+msgdtl["SignalData"]["ItemCode"]+', Station: '+str(msgdtl["SignalData"]["StationSequenceNumber"])+', TransactionResult: '+str(msgdtl["SignalData"]["TransactionResult"]))
		print("--------------------------------------------------")
		print("stop")
		sys.exit()



def business_rules(signalCode, itemCode, sequenceNumber):
	global situation
	global sys
	match = next((x for x in situation if x["ItemCode"]==itemCode), None)
	match2 = next((y for y in situation if y["StationSequenceNumber"] == sequenceNumber), None)

	itemInStation1 = []
	itemInStation2 = []
	itemInStation3 = []
	itemInStation4 = []
	itemInStation5 = []

	if sequenceNumber == 1 and itemCode not in itemInStation1:
    		itemInStation1.append(itemCode)
	elif sequenceNumber == 2 and itemCode in itemInStation1:
    		itemInStation2.append(itemCode)
	elif sequenceNumber == 3 and itemCode in itemInStation2 and itemCode in itemInStation1:
    		itemInStation3.append(itemCode)
	elif sequenceNumber == 4 and itemCode in itemInStation3 and itemCode in itemInStation1 and itemCode in itemInStation2:
    		itemInStation4.append(itemCode)
	elif sequenceNumber == 5 and itemCode in itemInStation4 and itemCode in itemInStation2  and itemCode in itemInStation3 and itemCode in itemInStation1:
    		itemInStation5.append(itemCode)
	else:
    		error_msg(itemCode,sequenceNumber)
			
######
	



#######
	# check that signal code matches sequence number
	######### check that message is not a duplicate, i.e. unit is already in location
	######### check that ID is done to unit which is not in the queue
	######### check that any other signal than ID is done to unit that is already in the queue
	######### check that one unit cannot overtake another
	# check that exit has the xxx flag

print('Defining callback function')

def callback(ch, method, properties, body):
	global channel
	global situation
	#my_cls=os.system('cls')

	msg=json.loads(body)
	if "SignalBody" not in msg:
    		
    		print("\n \n all units had wrapped completely")
    		sys.exit()
	signalCode=msg["SignalCode"]
	itemCode=msg["SignalBody"]["ItemCode"]
	seqNbr=msg["SignalBody"]["StationSequenceNumber"]
	kickOutFlag="False"
	if "KickOutFlag" in msg["SignalBody"]:
		kickOutFlag=msg["SignalBody"]["KickOutFlag"]

	print('Received Message: '+signalCode+', ItemCode: '+itemCode+', Station: '+str(seqNbr))

#	call rules fnct to check if the incoming msg make sense SSN 1st msg from ID , 1 unit in 1 station, unit can't overtake another, 
#	write fake tester script that will send the message with error and add a reply message containing the transaction code = to fals

	print("--------------------------------------------------------------- \n")
	business_rules(signalCode,itemCode,seqNbr)

	print("--------------------------------------------------------------- \n")


	match = next((x for x in situation if x["ItemCode"]==itemCode), None)

	if match != None:
		situation.remove(match)

	situation.append( { "ItemCode": itemCode, "StationSequenceNumber": seqNbr } )

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

	file = open("sample_reply.json", "r")
	rawmsg = file.read()
	file.close()
	reply = json.loads(rawmsg)

	hdrs=reply["Header"]
	mqmsgid=reply["MsgId"]
	msgtype=reply["Type"]
	msgdtl=reply["Body"]

	msgdtl["SignalCode"]=signalCode+'_RSP'
	msgdtl["SignalData"]["ItemCode"] = itemCode
	msgdtl["SignalData"]["StationSequenceNumber"] = seqNbr

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

	channel.basic_publish(exchange='(TIX Hub)',
                      routing_key=key,
                      body=json.dumps(msgdtl),
		properties=props,
		mandatory=False)

	print('Replied: '+msgdtl["SignalCode"]+', ItemCode: '+msgdtl["SignalData"]["ItemCode"]+', Station: '+str(msgdtl["SignalData"]["StationSequenceNumber"]))
	print("--------------------------------------------------")

print('Receiver starting')
print(' [*] Waiting for messages. To exit press CTRL+C')

channel.basic_consume(
    queue='tips-emulator', on_message_callback=callback, auto_ack=True)

channel.start_consuming()
