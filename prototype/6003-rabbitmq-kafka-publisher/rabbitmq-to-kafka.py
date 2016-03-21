import puka
import signal
import sys
from Queue import Queue
from threading import Thread
import json
from kafka import KafkaProducer
from kafka.common import KafkaError
import traceback

producer = KafkaProducer(bootstrap_servers=['10.21.49.9:32000'])

def signal_handler(signal, frame):
        print('You pressed Ctrl+C!')
        sys.exit(0)

def put_on_kafka(q):
    while True:
        try:
		raw = q.get()
		message = json.loads(raw)['2']['lst']
		jsid = message[6]
		adid = message[8]
		crid = message[10]
		kafka_message = str.encode(str(jsid) + '_' + str(adid) + '_' + str(crid))
            	future = producer.send('rabbitmq', str.encode(kafka_message)) 
        except Exception as e:
            print e
            traceback.print_exc()
            continue
 
q = Queue(10000)
num_threads = 2
 
for i in range(num_threads):
  worker = Thread(target=put_on_kafka, args=(q,))
  worker.setDaemon(True)
  worker.start()
 
consumer = puka.Client("amqp://geoworker-21.jiwiredev.com:5672")

# connect receiving party
receive_promise = consumer.connect()
consumer.wait(receive_promise)

# start waiting for messages, also those sent before (!), on the queue named rabbit
receive_promise = consumer.basic_consume(queue='RtbAiCounterWorker', no_ack=True)

print "Starting receiving!"

signal.signal(signal.SIGINT, signal_handler)
while True:
    try:
        received_message = consumer.wait(receive_promise)
        print "GOT: %r" % (received_message['body'],)
        q.put(received_message['body'])
    except Exception as e:
        print e
        traceback.print_exc()
        continue

q.join()
