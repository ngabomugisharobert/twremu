import pika
import json
import sys

mq_connect='localhost'

print('Receiver starting')
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

print('Defining callback function')

def callback(ch, method, properties, body):	
	global channel

	print(" [x] Received %r" % body)

	# Sleep here 2 seconds

	msg=json.loads(body)
	seqnbr=msg["SignalBody"]["StationSequenceNumber"]
	signalCode=msg["SignalCode"]

	file = open('sample_toipc.json',"r") 
	rawReply=file.read() 
	file.close() 

	reply=json.loads(rawReply)

	hdrs=reply["Header"]
	mqmsgid=reply["MsgId"]
	msgtype=reply["Type"]
	msgdtl=reply["Body"]

	msgdtl["SignalCode"]=signalCode+'_RSP'
	msgdtl["SignalData"]["StationSequenceNumber"] = seqnbr

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
	print('KEY=',key)

	print('sending')

	channel.basic_publish(exchange='(TIX Hub)',
                      routing_key=key,
                      body=json.dumps(msgdtl),
		properties=props,
		mandatory=False)


channel.basic_consume(
    queue='tips-emulator', on_message_callback=callback, auto_ack=True)

print('Receiver started')
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

