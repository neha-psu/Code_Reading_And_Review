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

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    arguments = ccloud_lib.parse_args()
    config_file = arguments.config_file
    topic = arguments.topic
    configuration = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': configuration['bootstrap.servers'],
        'sasl.mechanisms': configuration['sasl.mechanisms'],
        'security.protocol': configuration['security.protocol'],
        'sasl.username': configuration['sasl.username'],
        'sasl.password': configuration['sasl.password'],
    })

    # Create topic if needed
    ccloud_lib.create_topic(configuration, topic)

    delivered_records = 0
    breadcrumb_path = '/home/agrawal/examples/clients/cloud/python/sensor_data/2021-02-05.json'
    with open(breadcrumb_path) as file:
        breadcrumb_data = json.load(file)
        
    stop_event_path = '/home/agrawal/examples/clients/cloud/python/stop_event/2021-02-05.json'
    with open(stop_event_path) as file:
        stop_event_data = json.load(file)

    def acked(error, message):
        """
        Optional per-message on_delivery handler (triggered by poll() or flush())
        when a message has been successfully delivered or
        permanently failed delivery (after retries).
        :param error (String): error message in case of failed delivery
        :param message (String): successful delivery of the message
        :return: None
        """
        global delivered_records

        if error is not None:
            print(f'Failed to deliver message: {error}')
        else:
            delivered_records += 1
            print(f"""Produced record to topic {message.topic()} 
                    partition [{message.partition()}] @ offset {message.offset()}""")

    # For all the records in the breadcrumb data, set the json key as 'Breadcrumb'
    # and json value as the record itself. The "key" value will be helpful to consume 
    # the record from a particular stream. 
    # Finally, publish the record to the kafka topic.     
    for record in breadcrumb_data:
        record_key = 'Breadcrumb'
        record_value = json.dumps(record)
        producer.produce(topic, key = record_key.encode('utf-8'), value = record_value, \
            on_delivery = acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)

    # For all the records in the stop event data, set the json key as 'stop_event'
    # and json value as the record itself. Finally, publish the record to the kafka topic.   
    for record in stop_event_data:
        record_key = 'stop_event'
        record_value = json.dumps(record)
        producer.produce(topic, key = record_key.encode('utf-8'), value = record_value, \
            on_delivery = acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(0)

    # Adding flush() before exiting will make the client wait for any outstanding messages
    # in the Producer queue to be delivered to the broker.    
    producer.flush()
    print(f'{delivered_records} messages were produced to topic {topic}!')
