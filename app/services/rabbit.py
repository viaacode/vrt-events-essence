#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  @Author: Rudolf De Geijter
#
#  app/services/rabbit.py
#

import time

from viaa.configuration import ConfigParser
from viaa.observability import logging

import pika


class RabbitClient:
    def __init__(self):
        configParser = ConfigParser()
        self.log = logging.get_logger(__name__, config=configParser)
        self.rabbitConfig = configParser.app_cfg["rabbitmq"]

        self.credentials = pika.PlainCredentials(
            self.rabbitConfig["username"], self.rabbitConfig["password"]
        )

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.rabbitConfig["host"],
                port=self.rabbitConfig["port"],
                credentials=self.credentials,
            )
        )

        self.channel = self.connection.channel()

        self.__setup_rabbit_exchanges()

    def __setup_rabbit_exchanges(self):
        self.channel.exchange_declare(
            exchange=self.rabbitConfig["exchange"],
            exchange_type=self.rabbitConfig["exchange_type"],
            durable=True,
        )

        self.channel.queue_declare(
            queue=self.rabbitConfig["queue"],
            durable=True,
        )

        self.channel.queue_bind(
            exchange=self.rabbitConfig["exchange"],
            queue=self.rabbitConfig["queue"],
            routing_key=self.rabbitConfig["essence_linked_routing_key"],
        )

        self.channel.queue_bind(
            exchange=self.rabbitConfig["exchange"],
            queue=self.rabbitConfig["queue"],
            routing_key=self.rabbitConfig["essence_unlinked_routing_key"],
        )

        self.channel.queue_bind(
            exchange=self.rabbitConfig["exchange"],
            queue=self.rabbitConfig["queue"],
            routing_key=self.rabbitConfig["object_deleted_routing_key"],
        )

    def send_message(self, body, routing_key):
        try:
            self.channel.basic_publish(
                exchange=self.rabbitConfig["exchange"],
                routing_key=routing_key,
                body=body,
            )

            return True

        except pika.exceptions.AMQPConnectionError as ce:
            raise ce

    def listen(self, on_message_callback, queue=None):
        if queue is None:
            queue = self.rabbitConfig["queue"]

        try:
            while True:
                try:
                    channel = self.connection.channel()

                    channel.basic_consume(
                        queue=queue, on_message_callback=on_message_callback
                    )

                    channel.start_consuming()
                except pika.exceptions.StreamLostError:
                    self.logger.warning("RMQBridge lost connection, reconnecting...")
                    time.sleep(3)
                except pika.exceptions.ChannelWrongStateError:
                    self.logger.warning(
                        "RMQBridge wrong state in channel, reconnecting..."
                    )
                    time.sleep(3)
                except pika.exceptions.AMQPHeartbeatTimeout:
                    self.logger.warning(
                        "RMQBridge heartbeat timed out, reconnecting..."
                    )
                    time.sleep(3)

        except KeyboardInterrupt:
            self.channel.stop_consuming()

        self.connection.close()
