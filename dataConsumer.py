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

import json
import time

from confluent_kafka import Consumer
import ccloud_lib

from data_validation import validate
from data_load import postgres


if __name__ == '__main__':

    # Read arguments and configurations and initialize
    arguments = ccloud_lib.parse_args()
    config_file = arguments.config_file
    topic = arguments.topic
    configuration = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': configuration['bootstrap.servers'],
        'sasl.mechanisms': configuration['sasl.mechanisms'],
        'security.protocol': configuration['security.protocol'],
        'sasl.username': configuration['sasl.username'],
        'sasl.password': configuration['sasl.password'],
        'group.id': 'python_example_group_1',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0
    breadcrumb_list = []
    stop_event_list = []
    flag = 0
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                timeout = 10
                if flag == 0:
                    print('Waiting for message or event/error in poll()')
                    start = time.time()
                    flag = 1
                else:
                    print('Waiting for message or event/error in poll()')

                if time.time() > start + timeout:
                    break
                else:
                    continue
            elif message.error():
                print('error: {}'.format(message.error()))
            else:
                # Check for Kafka message
                record_key = message.key()
                if record_key == 'Breadcrumb':
                    record_value = message.value().decode('utf-8')
                    data = json.loads(record_value)
                    breadcrumb_list.append(data)
                    total_count += 1
                    #print("Consumed record with value {}.format(data))

                if record_key == 'stop_event':
                    record_value = message.value().decode('utf-8')
                    data = json.loads(record_value)
                    stop_event_list.append(data)
                    total_count += 1
                    #print("Consumed record with value {}.format(data))
    except KeyboardInterrupt:
        pass
    finally:
        if breadcrumb_list or stop_event_list:
            breadcrumb_json_data = json.dumps(breadcrumb_list, indent = 4)
            stop_event_json_data = json.dumps(stop_event_list, indent = 4)
            validate(breadcrumb_json_data, stop_event_json_data)
            postgres()
        # Leave group and commit final offsets
        consumer.close()
