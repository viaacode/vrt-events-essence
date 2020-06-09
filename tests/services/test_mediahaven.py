#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import BytesIO
from unittest.mock import patch

from lxml import etree

from app.services.mediahaven import MediahavenClient
from tests.resources.resources import load_xml_resource, construct_filename


class TestMediahaven:

    def test_construct_metadata_media_id(mock_mediahaven):
        cfg = {"mediahaven" : {"host": "host"}}
        client = MediahavenClient(cfg)

        # Load in XML schema
        schema = etree.XMLSchema(file=construct_filename("mhs.xsd"))

        # Parse essence event linked as tree
        xml = client._construct_metadata_media_id('media id')
        tree = etree.parse(BytesIO(xml.encode("utf-8")))

        # Assert validness according to schema
        is_xml_valid = schema.validate(tree)
        assert is_xml_valid
