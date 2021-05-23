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
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import ccloud_lib
from data_validation import *
from data_load import *
import time


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0
    breadcrumb_list = []
    stopevent_list = []
    flag =0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
               
                timeout = 10
                if flag == 0:
                    print("Waiting for message or event/error in poll()")
                    start = time.time()
                    flag = 1
                else:
                    print("Waiting for message or event/error in poll()")
                #print(time.time(), start, start + timeout)
                if time.time() > start + timeout:
                    #print("Hi")
                    #print(time.time(), start)
                    break
                else:
                    continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                if record_key == "Breadcrumb":
                    record_value = msg.value().decode('utf-8')
                    data = json.loads(record_value)
                    breadcrumb_list.append(data)
                    total_count += 1
                    #print("Consumed record with value {}.format(data))
                if record_key == "stop_event":
                    record_value = msg.value().decode('utf-8')
                    data = json.loads(record_value)
                    stopevent_list.append(data)
                    total_count += 1
                    #print("Consumed record with value {}.format(data))

    except KeyboardInterrupt:
        pass
    finally:
        if breadcrumb_list or stopevent_list:
            bc_json_data = json.dumps(breadcrumb_list, indent=4)
            se_json_data = json.dumps(stopevent_list, indent=4)
            validate(bc_json_data, se_json_data)
            #postgres()
        
        # Leave group and commit final offsets
        consumer.close()
