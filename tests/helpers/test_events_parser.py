#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from io import BytesIO

from lxml import etree

from app.helpers.events_parser import EssenceLinkedEvent
from tests.resources.essence_linked_events import essence_linked_event


class TestEssenceLinkedEvent:
    def test_essence_linked_event_xsd(self):
        """Test if the essence event linked xml is valid for the XML schema."""

        # Load in XML schema
        xsd_file = os.path.join(
            os.getcwd(), "tests", "resources", "essence_linked_event.xsd"
        )
        schema = etree.XMLSchema(file=xsd_file)

        # Parse essence event linked as tree
        tree = etree.parse(BytesIO(essence_linked_event))

        # Assert validness according to schema
        is_xml_valid = schema.validate(tree)
        assert is_xml_valid

    def test_essence_linked_event(self):
        event = EssenceLinkedEvent(essence_linked_event)
        assert event.timestamp == "2019-09-24T17:21:28.787+02:00"
        assert event.file == "file.mxf"
        assert event.media_id == "media id"
