#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import BytesIO
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
from lxml import etree
from requests.exceptions import HTTPError

from app.app import EventListener, NackException
from tests.resources.resources import load_xml_resource, construct_filename


@pytest.fixture
@patch('app.app.MediahavenClient')
@patch('app.app.RabbitClient')
def event_listener(rabbit_client, mh_client):
    """ Creates an event listener with mocked rabbit client and MH client"""
    return EventListener()


def test_handle_nack_exception(event_listener, caplog):
    mock_channel = MagicMock()
    delivery_tag = 1
    error_message = "error message"
    error_id = "error id"
    exception = NackException(error_message, error_id=error_id)

    event_listener._handle_nack_exception(exception, mock_channel, delivery_tag)
    # Assert nack being sent
    assert mock_channel.basic_nack.call_count == 1
    assert mock_channel.basic_nack.call_args[1]["delivery_tag"] == 1
    assert not mock_channel.basic_nack.call_args[1]["requeue"]

    # Assert error logged
    record = caplog.records[0]
    assert record.level == "error"
    assert record.message == error_message
    assert record.error_id == error_id


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
    with pytest.raises(NackException):
        event_listener._essence_linked_parse_event(essence_linked_event)


@patch.object(EventListener, "_handle_linked_event")
def test_handle_message_essence_linked(mock_handle_linked_event, event_listener):
    """ Tests if an essence linked event gets interpreted as such """
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


@patch.object(EventListener, '_handle_linked_event', side_effect=NackException("error"))
@patch.object(EventListener, '_handle_nack_exception', autospec=True)
def test_handle_message_essence_linked_nack(mock_nack, mock_event, event_listener):
    """ When a nack exception occurs when handling a essence linked event
    it should handle the exception accordingly
    """
    routing_key = "essence_linked_routing_key"
    event_listener.essence_linked_rk = routing_key

    mock_channel = MagicMock()
    mock_method = MagicMock()
    mock_method.delivery_tag = 1
    mock_method.routing_key = routing_key
    essence_linked_event = load_xml_resource("essenceLinkedEvent.xml")

    event_listener.handle_message(mock_channel, mock_method, None, essence_linked_event)

    assert mock_nack.call_count == 1
    assert mock_nack.call_args[0][1].message == "error"
    assert mock_nack.call_args[0][2] == mock_channel
    assert mock_nack.call_args[0][3] == 1


@patch.object(EventListener, "_essence_linked_parse_event")
@patch.object(EventListener, "_essence_linked_get_fragment")
@patch.object(EventListener, "_essence_linked_retrieve_umid")
@patch.object(EventListener, "_essence_linked_create_fragment")
@patch.object(EventListener, "_essence_linked_retrieve_fragment_id")
@patch.object(EventListener, "_essence_linked_add_metadata", return_value=False)
@patch.object(EventListener, "_generate_get_metadata_request_xml")
def test_handle_linked_event_update_false(
    mock_generate_get_metadata_request_xml,
    mock_add_metadata,
    mock_retrieve_fragment_id,
    mock_create_fragment,
    mock_retrieve_umid,
    mock_get_fragment,
    mock_parse_event,
    event_listener
):
    """ If the metadata update call to MH return a status code in the 200 range
    but not a 204, it will be seen as unsuccessful. In this case it should
    stop the handling flow and send a nack to rabbit
    """
    with pytest.raises(NackException):
        event_listener._handle_linked_event("irrelevant")
    assert mock_parse_event.call_count == 1
    assert mock_get_fragment.call_count == 1
    assert mock_retrieve_umid.call_count == 1
    assert mock_create_fragment.call_count == 1
    assert mock_retrieve_fragment_id.call_count == 1
    assert mock_add_metadata.call_count == 1
    assert mock_generate_get_metadata_request_xml.call_count == 0


def test_essence_linked_get_fragment(event_listener):
    mh_client_mock = event_listener.mh_client
    mh_client_mock.get_fragment.return_value = {}

    assert event_listener._essence_linked_get_fragment("file") == {}
    assert mh_client_mock.get_fragment.call_count == 1
    assert mh_client_mock.get_fragment.call_args[0][0] == "s3_object_key"
    assert mh_client_mock.get_fragment.call_args[0][1] == "file"


def test_essence_linked_get_fragment_http_error(event_listener):
    mh_client_mock = event_listener.mh_client
    # Raise a HTTP Error when calling method
    event_listener.mh_client.get_fragment.side_effect = HTTPError

    with pytest.raises(NackException):
        event_listener._essence_linked_get_fragment("file")
    assert mh_client_mock.get_fragment.call_count == 1
    assert mh_client_mock.get_fragment.call_args[0][0] == "s3_object_key"
    assert mh_client_mock.get_fragment.call_args[0][1] == "file"


def test_essence_linked_retrieve_umid(event_listener):
    object_id = "object id"
    fragment = {"MediaDataList": [{"Internal": {"MediaObjectId": object_id}}]}
    assert event_listener._essence_linked_retrieve_umid(fragment) == object_id


def test_essence_linked_retrieve_umid_key_error(event_listener):
    object_id = "object id"
    fragment = {"wrong": object_id}
    with pytest.raises(NackException):
        event_listener._essence_linked_retrieve_umid(fragment)


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

    with pytest.raises(NackException):
        event_listener._essence_linked_create_fragment(umid)
    assert mh_client_mock.create_fragment.call_count == 1
    assert mh_client_mock.create_fragment.call_args[0][0] == umid


def test_essence_linked_retrieve_fragment_id(event_listener):
    fragment_id = "fragment id"
    response = {"Internal": {"FragmentId": fragment_id}}
    assert event_listener._essence_linked_retrieve_fragment_id(response) == fragment_id


def test_essence_linked_retrieve_fragment_id_key_error(event_listener):
    fragment_id = "fragment id"
    response = {"wrong": fragment_id}
    with pytest.raises(NackException):
        event_listener._essence_linked_retrieve_fragment_id(response)


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

    with pytest.raises(NackException):
        event_listener._essence_linked_add_metadata(fragment_id, media_id)
    assert mh_client_mock.add_metadata_to_fragment.call_count == 1
    assert mh_client_mock.add_metadata_to_fragment.call_args[0][0] == fragment_id
    assert mh_client_mock.add_metadata_to_fragment.call_args[0][1] == media_id


@patch.object(EventListener, "_handle_unlinked_event")
def test_handle_message_essence_unlinked(mock_handle_unlinked_event, event_listener):
    """ Tests if an essence unlinked event gets interpreted as such """
    routing_key = "essence_unlinked_routing_key"
    event_listener.essence_unlinked_rk = routing_key

    mock_channel = MagicMock()
    mock_method = MagicMock()
    mock_method.delivery_tag = 1
    mock_method.routing_key = routing_key
    essence_unlinked_event = load_xml_resource("essenceUnlinkedEvent.xml")

    event_listener.handle_message(mock_channel, mock_method, None, essence_unlinked_event)

    assert mock_handle_unlinked_event.call_count == 1
    assert mock_handle_unlinked_event.call_args[0][0] == essence_unlinked_event

    assert mock_channel.basic_ack.call_count == 1
    assert mock_channel.basic_ack.call_args[1]["delivery_tag"] == 1


@patch.object(EventListener, '_handle_unlinked_event', side_effect=NackException("error"))
@patch.object(EventListener, '_handle_nack_exception', autospec=True)
def test_handle_message_essence_unlinked_nack(mock_nack, mock_event, event_listener):
    """ When a nack exception occurs when handling a essence unlinked event
    it should handle the exception accordingly.
    """
    routing_key = "essence_unlinked_routing_key"
    event_listener.essence_unlinked_rk = routing_key

    mock_channel = MagicMock()
    mock_method = MagicMock()
    mock_method.delivery_tag = 1
    mock_method.routing_key = routing_key
    essence_unlinked_event = load_xml_resource("essenceUnlinkedEvent.xml")

    event_listener.handle_message(mock_channel, mock_method, None, essence_unlinked_event)

    assert mock_nack.call_count == 1
    assert mock_nack.call_args[0][1].message == "error"
    assert mock_nack.call_args[0][2] == mock_channel
    assert mock_nack.call_args[0][3] == 1


def test_essence_unlinked_parse_event(event_listener):
    essence_unlinked_event = load_xml_resource("essenceUnlinkedEvent.xml")
    event = event_listener._essence_unlinked_parse_event(essence_unlinked_event)
    assert event is not None
    assert event.media_id == "media id"


def test_essence_unlinked_parse_event_invalid(event_listener):
    essence_unlinked_event = b""
    with pytest.raises(NackException):
        event_listener._essence_unlinked_parse_event(essence_unlinked_event)


def test_essence_unlinked_get_fragment(event_listener):
    mh_client_mock = event_listener.mh_client
    mh_client_mock.get_fragment.return_value = {}

    assert event_listener._essence_unlinked_get_fragment("media_id") == {}
    assert mh_client_mock.get_fragment.call_count == 1
    assert mh_client_mock.get_fragment.call_args[0][0] == "dc_identifier_localid"
    assert mh_client_mock.get_fragment.call_args[0][1] == "media_id"


def test_essence_unlinked_get_fragment_http_error(event_listener):
    mh_client_mock = event_listener.mh_client
    # Raise a HTTP Error when calling method
    event_listener.mh_client.get_fragment.side_effect = HTTPError

    with pytest.raises(NackException):
        event_listener._essence_unlinked_get_fragment("media_id")
    assert mh_client_mock.get_fragment.call_count == 1
    assert mh_client_mock.get_fragment.call_args[0][0] == "dc_identifier_localid"
    assert mh_client_mock.get_fragment.call_args[0][1] == "media_id"


def test_essence_unlinked_retrieve_fragment_id(event_listener):
    fragment_id = "fragment id"
    response = {"MediaDataList": [{"Internal": {"FragmentId": fragment_id}}]}
    assert event_listener._essence_unlinked_retrieve_fragment_id(response) == fragment_id


def test_essence_unlinked_retrieve_fragment_id_key_error(event_listener):
    fragment_id = "fragment id"
    response = {"wrong": fragment_id}
    with pytest.raises(NackException):
        event_listener._essence_unlinked_retrieve_fragment_id(response)


def test_essence_unlinked_delete_fragment(event_listener):
    mh_client_mock = event_listener.mh_client
    fragment_id = "fragment id"

    # Return True, which means the action was successful
    mh_client_mock.delete_fragment.return_value = True

    assert event_listener._essence_unlinked_delete_fragment(fragment_id)
    assert mh_client_mock.delete_fragment.call_count == 1
    assert mh_client_mock.delete_fragment.call_args[0][0] == fragment_id


def test_essence_unlinked_delete_fragment_http_error(event_listener):
    mh_client_mock = event_listener.mh_client
    fragment_id = "fragment id"

    # Raise a HTTP Error when calling method
    mh_client_mock.delete_fragment.side_effect = HTTPError

    with pytest.raises(NackException):
        event_listener._essence_unlinked_delete_fragment(fragment_id)
    assert mh_client_mock.delete_fragment.call_count == 1
    assert mh_client_mock.delete_fragment.call_args[0][0] == fragment_id


@patch.object(EventListener, "_essence_unlinked_parse_event")
@patch.object(EventListener, "_essence_unlinked_get_fragment")
@patch.object(EventListener, "_essence_unlinked_retrieve_fragment_id")
@patch.object(EventListener, "_essence_unlinked_delete_fragment",  return_value=False)
def test_handle_unlinked_event_delete_false(
    mock_delete_fragment,
    mock_retrieve_umid,
    mock_get_fragment,
    mock_parse_event,
    event_listener
):
    """ If the delete fragment call to MH returns a status code in the 200 range
    but not a 204, it will be seen as unsuccessful. In this case it should
    stop the handling flow and send a nack to rabbit
    """
    with pytest.raises(NackException):
        event_listener._handle_unlinked_event("irrelevant")
    assert mock_parse_event.call_count == 1
    assert mock_get_fragment.call_count == 1
    assert mock_retrieve_umid.call_count == 1
    assert mock_delete_fragment.call_count == 1


def test_handle_message_unknown_routing_key(event_listener, caplog):
    mock_channel = MagicMock()
    mock_method = MagicMock()
    mock_method.delivery_tag = 1
    mock_method.routing_key = "unknown"
    message = "irrelevant"

    event_listener.handle_message(mock_channel, mock_method, None, message)

    captured = caplog.text
    assert "Unknown routing key: unknown" in captured
    assert mock_channel.basic_nack.call_count == 1
    assert mock_channel.basic_nack.call_args[1]["delivery_tag"] == 1
    assert not mock_channel.basic_nack.call_args[1]["requeue"]
