#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import BytesIO
from unittest.mock import patch

from lxml import etree

from app.services.mediahaven import MediahavenClient, NSMAP
from tests.resources.resources import load_xml_resource, construct_filename


class TestMediahaven:
    def test_construct_metadata(mock_mediahaven):
        cfg = {"mediahaven": {"host": "host"}}
        client = MediahavenClient(cfg)
        media_id = "media id"
        pid = "pid"

        # Load in XML schema
        schema = etree.XMLSchema(file=construct_filename("mhs.xsd"))

        # Parse essence event linked as tree
        xml = client._construct_metadata(media_id, pid)
        tree = etree.parse(BytesIO(xml.encode("utf-8")))

        # Assert validness according to schema
        is_xml_valid = schema.validate(tree)
        assert is_xml_valid

        # Assert values in tags
        l_id_element = tree.xpath("mhs:Dynamic/dc_identifier_localid", namespaces=NSMAP)
        assert l_id_element[0].text == media_id
        m_id_element = tree.xpath(
            "mhs:Dynamic/dc_identifier_localids/MEDIA_ID", namespaces=NSMAP
        )
        assert m_id_element[0].text == media_id
        assert tree.xpath("mhs:Dynamic/PID", namespaces=NSMAP)[0].text == pid
