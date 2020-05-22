#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest


@pytest.fixture
def mock_rabbit(mocker):
    def mock_init(self):
        print("Initiating Rabbit connection.")
        pass

    def mock_send_message(self, body, routing_key):
        print("Sending Rabbit message.")
        pass

    def mock_listen(self, on_message_callback, queue=None):
        print("Listening for Rabbit messages.")
        pass

    from app.services.rabbit import RabbitClient

    mocker.patch.object(RabbitClient, "__init__", mock_init)
    mocker.patch.object(RabbitClient, "send_message", mock_send_message)
    mocker.patch.object(RabbitClient, "listen", mock_listen)
