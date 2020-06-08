#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from io import BytesIO
from datetime import datetime
from unittest.mock import patch, MagicMock

from lxml import etree

from app.app import EventListener
from tests.resources.essence_linked_events import essence_linked_event
from tests.resources.mocks import mock_rabbit


def test_generate_get_metadata_request_xml(mock_rabbit):
    event_listener = EventListener()
    # Create getMetadataRequest XML
    xml = event_listener._generate_get_metadata_request_xml(
        datetime.now().isoformat(), "cor", "media"
    )

    # Load in XML schema
    xsd_fn = os.path.join(
        os.path.dirname(__file__), "resources", "get_metadata_request.xsd"
    )
    schema = etree.XMLSchema(file=xsd_fn)

    # Parse getMetadataRequest XML as tree
    tree = etree.parse(BytesIO(xml.encode("utf-8")))

    # Assert validness according to schema
    is_xml_valid = schema.validate(tree)
    assert is_xml_valid


@patch.object(EventListener, "_handle_linked_event")
def test_handle_message_essence_linked(mock_handle_linked_event, mock_rabbit):
    # ARRANGE
    routing_key = "essence_linked_routing_key"
    eventListener = EventListener()
    eventListener.essence_linked_rk = routing_key

    mock_channel = MagicMock()
    mock_method = MagicMock()
    mock_method.delivery_tag = 1
    mock_method.routing_key = routing_key

    # ACT
    eventListener.handle_message(mock_channel, mock_method, None, essence_linked_event)

    # ASSERT
    assert mock_handle_linked_event.call_count == 1
    assert mock_handle_linked_event.call_args[0][0] == essence_linked_event

    assert mock_channel.basic_ack.call_count == 1
    assert mock_channel.basic_ack.call_args[1]["delivery_tag"] == 1
