#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

import json
from time import sleep
import random

from confluent_kafka import Producer, KafkaError
import ccloud_lib
import urllib.request


def produce_breadcrumb_records():
    pass

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
    })

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0
    breadcrumb_path = '/home/agrawal/examples/clients/cloud/python/sensor_data/2021-02-05.json'
    with open(breadcrumb_path) as file:
        breadcrumb_data = json.load(file)
        
    stopEvent_path = '/home/agrawal/examples/clients/cloud/python/stop_event/2021-02-05.json'
    with open(stopEvent_path) as file:
        stopEvent_data = json.load(file)

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic "+str(msg.topic())+" partition",\
                    "["+str(msg.partition())+"] @ offset "+str(msg.offset()))
        
    for val in breadcrumb_data:
        record_key = "Breadcrumb"
        record_value = json.dumps(val)
        producer.produce(topic, key=record_key.encode('utf-8'), value=record_value, on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)

    for val in stopEvent_data:
        record_key = "stop_event"
        record_value = json.dumps(val)
        producer.produce(topic, key=record_key.encode('utf-8'), value=record_value, on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)
        
    producer.flush()
    print("{} messages were produced to topic {}!".format(delivered_records, topic))
