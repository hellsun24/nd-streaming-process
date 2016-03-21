from kafka import KafkaConsumer
import happybase
from threading import Thread
import traceback

def put_to_maprdb(message, table, metaTable, jobName):
    try:
        row_key = message.value
        row = table.row(row_key)
        if row:
            pass
        else:
            table.put(row_key, {'cf:e': '1'})
            values = message.value.split('_')
            adid = str(values[1])
            crid = str(values[2])
            meta_count_row_key = str(message.partition) + '_' + adid + '_' + crid + '_' + jobName + '_6003_count'
            metaTable.counter_inc(meta_count_row_key,'cf:count',value=1)
            meta_offset_row_key = str(message.partition) + '_' + jobName + '_offset'
            metaTable.put(meta_offset_row_key, {'cf:offset':str(message.offset)})
    except Exception as e:
        print e
        traceback.print_exc()

topic = 'rabbitmq'
jobName = 'prototype'
connection = happybase.Connection('10.21.49.3')
connection.open()
daily_6003 = connection.table('/mapr/my.cluster.com/user/bsun/tables/daily_6003')
daily_6003_meta = connection.table('/mapr/my.cluster.com/user/bsun/tables/daily_6003_meta')

consumer = KafkaConsumer(topic,
                         bootstrap_servers=['10.21.49.9:32000'])
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    put_to_maprdb(message, daily_6003, daily_6003_meta, jobName)
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
