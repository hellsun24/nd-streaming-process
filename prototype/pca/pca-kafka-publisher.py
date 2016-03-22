#!/usr/bin/python

import puka
import signal
import sys
from Queue import Queue
from threading import Thread
import json
from kafka import KafkaProducer
from kafka.common import KafkaError
import traceback
from random import randint
import time

producer = KafkaProducer(bootstrap_servers=['10.21.49.9:32000'])

def r2():
    return randint(0,1)

def r3():
    return randint(0,2)

def r6():
    return randint(0,5)

def put_on_kafka(m):
    try:
        producer.send('pca', str.encode(m))
    except Exception as e:
        print e
        traceback.print_exc()

while True:
    try:
        avd_id = ['tmone', 'xmone', 'ymone']
        campaign_id = [20647, 20648, 20649]
        inv_type = ['mapp','mweb','online','tv','ooh','mail','audio']
        creative = ['aaa', 'bbb', 'ccc']
        plcmnt = ['sanfrancisco', 'lasvegas', 'seattle']
        aud = ['luxury', 'moveup']

        message = avd_id[r3] + '_' + campaign_id[r3] + '_' + inv_type[r6] + '_' + creative[r3] + '_' + plcmnt[r3] + '_' + aud[r2]
        put_on_kafka(message)
        time.sleep(1)
    except Exception as e:                                          
        print e                                                     
        traceback.print_exc()                                       
        continue                                                    
