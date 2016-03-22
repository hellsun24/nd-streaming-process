from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from ndspark import NDS
from pyspark.streaming.kafka import KafkaUtils
import csv,sys
import pytz
import time
import pandas as pd
from datetime import datetime, date, timedelta
from pyspark.sql import HiveContext, SQLContext, Row
from pyspark.sql.types import *
from operator import add
from humanize import intcomma, naturalsize
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL as SqlAlchemyURL
from ndt.spark.context import create_spark_context
import happybase
import traceback

TZ_NY = pytz.timezone('America/New_York')
now = TZ_NY.normalize(pytz.UTC.localize(datetime.utcnow()))
nows = now.strftime("%Y-%m-%d %H:%M:%S")
jobName = 'rtb-sync-impression-spark-streaming'

def createNewConnection(host):
    connection = happybase.Connection(host)
    connection.open()
    return connection

def put_to_maprdb(iter):
    jobName = 'rsi'
    maprDbHost = "10.21.49.3"
    tableName = "/mapr/my.cluster.com/user/bsun/tables/daily_6003"
    tableMetaName = "/mapr/my.cluster.com/user/bsun/tables/daily_6003_meta"
    connection = createNewConnection(maprDbHost)
    table = connection.table(tableName)
    metaTable = connection.table(tableMetaName)

    for record in iter:
        try:
            row_key = record
            row = table.row(row_key)
            if row:
                continue
            else:
                table.put(row_key, {'cf:e': '1'})
                values = record.split('_')
                adid = str(values[1])
                crid = str(values[2])
                source = str(values[3])
                meta_count_row_key = adid + '_' + crid + '_' + jobName + '_6003_count_' + source
                metaTable.counter_inc(meta_count_row_key,'cf:count',value=1)
                #meta_offset_row_key = str(record.partition) + '_' + jobName + '_offset_' + source 
                #metaTable.put(meta_offset_row_key, {'cf:offset':str(message.offset)})
        except Exception as e:
            print e
            traceback.print_exc()
    connection.close()

def mapper(row):
    try:
        key = str(row[0])
        value = str(row[1])
        if key and key == "syslog":
            values = value.split(" ")
            aid = str(values[7])
            crid = str(values[9])
            jsid = str(values[5])
            return jsid + '_' + aid + '_' + crid + '_syslog'
        else:
            return value + '_rabbitmq'
    except:
        return None
 
nds = NDS(jobName, cores=50, executor_memory='4g')
ssc = StreamingContext(nds.sc, 2)

brokers = "10.21.49.9:32000"
directKafkaStream = KafkaUtils.createDirectStream(ssc, ["rabbitmq", "syslog"], {"metadata.broker.list": brokers})

rdd = directKafkaStream.map(mapper).filter(lambda row: row)
rdd.pprint()

rdd.foreachRDD (lambda x : x.foreachPartition(put_to_maprdb))

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

