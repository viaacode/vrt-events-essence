#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import BytesIO
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
from lxml import etree
from requests.exceptions import HTTPError

from app.app import EventListener
from tests.resources.resources import load_xml_resource, construct_filename


@pytest.fixture
@patch('app.app.MediahavenClient')
@patch('app.app.RabbitClient')
def event_listener(rabbit_client, mh_client):
    """ Creates an event listener with mocked rabbit client and MH client"""
    return EventListener()


def test_generate_get_metadata_request_xml(event_listener):
    # Create getMetadataRequest XML
    xml = event_listener._generate_get_metadata_request_xml(
        datetime.now().isoformat(), "cor", "media"
    )

    # Load in XML schema
    schema = etree.XMLSchema(file=construct_filename("getMetadataRequest.xsd"))

    # Parse getMetadataRequest XML as tree
    tree = etree.parse(BytesIO(xml.encode("utf-8")))

    # Assert validness according to schema
    is_xml_valid = schema.validate(tree)
    assert is_xml_valid


def test_essence_linked_parse_event(event_listener):
    essence_linked_event = load_xml_resource("essenceLinkedEvent.xml")
    event = event_listener._essence_linked_parse_event(essence_linked_event)
    assert event is not None
    assert event.file == "file.mxf"


def test_essence_linked_parse_event_invalid(event_listener):
    essence_linked_event = b""
    event = event_listener._essence_linked_parse_event(essence_linked_event)
    assert event is None


@patch.object(EventListener, "_handle_linked_event")
def test_handle_message_essence_linked(mock_handle_linked_event, event_listener):
    routing_key = "essence_linked_routing_key"
    event_listener.essence_linked_rk = routing_key

    mock_channel = MagicMock()
    mock_method = MagicMock()
    mock_method.delivery_tag = 1
    mock_method.routing_key = routing_key
    essence_linked_event = load_xml_resource("essenceLinkedEvent.xml")

    event_listener.handle_message(mock_channel, mock_method, None, essence_linked_event)

    assert mock_handle_linked_event.call_count == 1
    assert mock_handle_linked_event.call_args[0][0] == essence_linked_event

    assert mock_channel.basic_ack.call_count == 1
    assert mock_channel.basic_ack.call_args[1]["delivery_tag"] == 1


def test_essence_linked_get_fragment(event_listener):
    mh_client_mock = event_listener.mh_client
    mh_client_mock.get_fragment.return_value = {}

    assert event_listener._essence_linked_get_fragment("frag_id") == {}
    assert mh_client_mock.get_fragment.call_count == 1
    assert mh_client_mock.get_fragment.call_args[0][0] == "s3_object_key"
    assert mh_client_mock.get_fragment.call_args[0][1] == "frag_id"


def test_essence_linked_get_fragment_http_error(event_listener):
    mh_client_mock = event_listener.mh_client
    # Raise a HTTP Error when calling method
    event_listener.mh_client.get_fragment.side_effect = HTTPError

    assert event_listener._essence_linked_get_fragment("frag_id") is None
    assert mh_client_mock.get_fragment.call_count == 1
    assert mh_client_mock.get_fragment.call_args[0][0] == "s3_object_key"
    assert mh_client_mock.get_fragment.call_args[0][1] == "frag_id"


def test_essence_linked_retrieve_umid(event_listener):
    object_id = "object id"
    fragment = {"MediaDataList": [{"Internal": {"MediaObjectId": object_id}}]}
    assert event_listener._essence_linked_retrieve_umid(fragment) == object_id


def test_essence_linked_retrieve_umid_key_error(event_listener):
    object_id = "object id"
    fragment = {"wrong": object_id}
    assert event_listener._essence_linked_retrieve_umid(fragment) is None


def test_essence_linked_create_fragment(event_listener):
    mh_client_mock = event_listener.mh_client
    fragment_id = "fragment id"
    umid = "umid"
    fragment_response = {"fragment_id": fragment_id}
    mh_client_mock.create_fragment.return_value = fragment_response

    assert event_listener._essence_linked_create_fragment(umid) == fragment_response
    assert mh_client_mock.create_fragment.call_count == 1
    assert mh_client_mock.create_fragment.call_args[0][0] == umid


def test_essence_linked_create_fragment_http_error(event_listener):
    mh_client_mock = event_listener.mh_client
    umid = "umid"

    # Raise a HTTP Error when calling method
    mh_client_mock.create_fragment.side_effect = HTTPError

    assert event_listener._essence_linked_create_fragment(umid) is None
    assert mh_client_mock.create_fragment.call_count == 1
    assert mh_client_mock.create_fragment.call_args[0][0] == umid


def test_essence_linked_retrieve_fragment_id(event_listener):
    fragment_id = "fragment id"
    response = {"Internal": {"FragmentId": fragment_id}}
    assert event_listener._essence_linked_retrieve_fragment_id(response) == fragment_id


def test_essence_linked_retrieve_fragment_id_key_error(event_listener):
    fragment_id = "fragment id"
    response = {"wrong": fragment_id}
    assert event_listener._essence_linked_retrieve_fragment_id(response) is None


def test_essence_linked_add_metadata(event_listener):
    mh_client_mock = event_listener.mh_client
    fragment_id = "fragment id"
    media_id = "media id"

    # Return True, which means the action was successful
    mh_client_mock.add_metadata_to_fragment.return_value = True

    assert event_listener._essence_linked_add_metadata(fragment_id, media_id)
    assert mh_client_mock.add_metadata_to_fragment.call_count == 1
    assert mh_client_mock.add_metadata_to_fragment.call_args[0][0] == fragment_id
    assert mh_client_mock.add_metadata_to_fragment.call_args[0][1] == media_id


def test_essence_linked_add_metadata_http_error(event_listener):
    mh_client_mock = event_listener.mh_client
    fragment_id = "fragment id"
    media_id = "media id"

    # Raise a HTTP Error when calling method
    mh_client_mock.add_metadata_to_fragment.side_effect = HTTPError

    assert not event_listener._essence_linked_add_metadata(fragment_id, media_id)
    assert mh_client_mock.add_metadata_to_fragment.call_count == 1
    assert mh_client_mock.add_metadata_to_fragment.call_args[0][0] == fragment_id
    assert mh_client_mock.add_metadata_to_fragment.call_args[0][1] == media_id
