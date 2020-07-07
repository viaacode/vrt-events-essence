#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import BytesIO
from unittest.mock import patch, MagicMock

import pytest
from lxml import etree

from app.helpers.retry import NUMBER_OF_TRIES
from app.services.mediahaven import MediahavenClient, NSMAP
from tests.resources.resources import construct_filename


class TestMediahaven:
    @pytest.fixture
    @patch.object(
        MediahavenClient,
        "_MediahavenClient__get_token",
        return_value={"access_token": "Bear with me"},
    )
    def mediahaven_client(self, mhs_get_token_mock):
        mh_config_dict = {
            "mediahaven": {
                "host": "mediahaven",
                "username": "user",
                "password": "pass",
            }
        }

        mhs = MediahavenClient(mh_config_dict)
        # Patch out __get_token so it doesn't send a request to MediaHaven
        mhs._MediahavenClient__get_token = mhs_get_token_mock
        return mhs

    def test_construct_metadata(self, mediahaven_client):
        media_id = "media id"
        pid = "pid"

        # Load in XML schema
        schema = etree.XMLSchema(file=construct_filename("mhs.xsd"))

        # Create the sidecar XML
        xml = mediahaven_client._construct_metadata(media_id, pid)
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

    @pytest.mark.parametrize(
        "status, code, message",
        [(404, "ENOTFND", "Resource cannot be found"), (403, "ENOACCES", "User has no access")]
    )
    @patch('requests.post')
    @patch('time.sleep')
    def test_add_metadata_to_fragment_retry(self, sleep_mock, update_mock, status, code, message, mediahaven_client):
        # Construct response message
        response = {
            "status": status,
            "message": message,
            "code": code,
        }

        # Mock response data
        response_mock = MagicMock()
        response_mock.json.return_value = response
        response_mock.status_code = status
        update_mock.return_value = response_mock

        fragment_id = "fragment_id"
        media_id = "media_id"
        pid = "PID"

        assert not mediahaven_client.add_metadata_to_fragment(fragment_id, media_id, pid)
        assert sleep_mock.call_count == NUMBER_OF_TRIES
