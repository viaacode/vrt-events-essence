#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from io import BytesIO
from datetime import datetime

from lxml import etree

from app.app import EventListener
from tests.resources.mocks import mock_rabbit


def test_generate_get_metadata_request_xml(mock_rabbit, mocker):
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
