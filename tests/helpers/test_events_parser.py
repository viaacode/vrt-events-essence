#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from io import BytesIO

from lxml import etree

from app.helpers.events_parser import EssenceLinkedEvent
from tests.resources.resources import load_xml_resource, construct_filename


class TestEssenceLinkedEvent:
    def test_essence_linked_event_xsd(self):
        """Test if the essence event linked xml is valid for the XML schema."""

        # Load in XML schema
        schema = etree.XMLSchema(file=construct_filename("essenceLinkedEvent.xsd"))

        # Parse essence event linked as tree
        tree = etree.parse(BytesIO(load_xml_resource("essenceLinkedEvent.xml")))

        # Assert validness according to schema
        is_xml_valid = schema.validate(tree)
        assert is_xml_valid

    def test_essence_linked_event(self):
        event = EssenceLinkedEvent(load_xml_resource("essenceLinkedEvent.xml"))
        assert event.timestamp == "2019-09-24T17:21:28.787+02:00"
        assert event.file == "file.mxf"
        assert event.media_id == "media id"
