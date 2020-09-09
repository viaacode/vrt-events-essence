#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC
from io import BytesIO
from datetime import datetime
from unittest.mock import patch, MagicMock

import pytest
from lxml import etree
from requests.exceptions import HTTPError, RequestException

from app.app import (
    EssenceLinkedHandler,
    EssenceUnlinkedHandler,
    EventListener,
    NackException,
    ObjectDeletedHandler,
    UnknownRoutingKeyHandler,
)
from app.helpers.events_parser import EssenceEvent, InvalidEventException
from tests.resources.resources import load_resource, construct_filename


@pytest.fixture
def http_error():
    response = MagicMock()
    response.status_code = 400
    response.text = "error"
    return HTTPError(response=response)


class TestEventListener:
    @pytest.fixture
    @patch('app.app.PIDService')
    @patch('app.app.MediahavenClient')
    @patch('app.app.RabbitClient')
    def event_listener(self, rabbit_client, mh_client, pid_service):
        """ Creates an event listener with mocked rabbit client, MH client and
        PID Service.
        """
        return EventListener()

    def test_handle_nack_exception(self, event_listener, caplog):
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

    @patch('time.sleep')
    def test_handle_nack_exception_requeue(self, time_mock, event_listener, caplog):
        mock_channel = MagicMock()
        delivery_tag = 1
        error_message = "error message"
        error_id = "error id"
        exception = NackException(error_message, requeue=True, error_id=error_id)

        event_listener._handle_nack_exception(exception, mock_channel, delivery_tag)

        # Assert nack being sent
        assert mock_channel.basic_nack.call_count == 1
        assert mock_channel.basic_nack.call_args[1]["delivery_tag"] == 1
        assert mock_channel.basic_nack.call_args[1]["requeue"]

        # Assert delay when resending on queue
        assert time_mock.call_count == 1
        assert time_mock.call_args[0][0] == 10

        # Assert error logged
        record = caplog.records[0]
        assert record.level == "error"
        assert record.message == error_message
        assert record.error_id == error_id

    def test_calculate_handler_essence_linked(self, event_listener):
        routing_key = "essence_linked_routing_key"
        event_listener.essence_linked_rk = routing_key

        handler = event_listener._calculate_handler(routing_key)
        assert type(handler) == EssenceLinkedHandler
        assert handler.mh_client == event_listener.mh_client
        assert handler.rabbit_client == event_listener.rabbit_client
        assert handler.routing_key == event_listener.get_metadata_rk

    def test_calculate_handler_essence_unlinked(self, event_listener):
        routing_key = "essence_unlinked_routing_key"
        event_listener.essence_unlinked_rk = routing_key

        handler = event_listener._calculate_handler(routing_key)
        assert type(handler) == EssenceUnlinkedHandler
        assert handler.mh_client == event_listener.mh_client

    def test_calculate_handler_object_deleted(self, event_listener):

        routing_key = "object_deleted_routing_key"
        event_listener.object_deleted_rk = routing_key

        handler = event_listener._calculate_handler(routing_key)
        assert type(handler) == ObjectDeletedHandler
        assert handler.mh_client == event_listener.mh_client

    def test_calculate_handler_unknown_routing_key(self, event_listener):
        routing_key = "unknown_routing_key"

        handler = event_listener._calculate_handler(routing_key)
        assert type(handler) == UnknownRoutingKeyHandler
        assert handler.routing_key == routing_key

    @patch.object(EventListener, "_calculate_handler")
    def test_handle_message(self, mock_calculate_handler, event_listener):
        routing_key = "routing_key"

        mock_channel = MagicMock()
        mock_method = MagicMock()
        mock_method.delivery_tag = 1
        mock_method.routing_key = routing_key
        event = b'event'

        event_listener.handle_message(mock_channel, mock_method, None, event)

        # Check if calculate handler method has been called
        assert mock_calculate_handler.call_count == 1
        assert mock_calculate_handler.call_args[0][0] == routing_key

        # Check if handle event of handler has been called
        handler_mock = mock_calculate_handler()
        assert handler_mock.handle_event.call_count == 1
        assert handler_mock.handle_event.call_args[0][0] == event

        # Check if message has been sent to queue
        assert mock_channel.basic_ack.call_count == 1
        assert mock_channel.basic_ack.call_args[1]["delivery_tag"] == 1
        assert mock_channel.basic_nack.call_count == 0

    @patch.object(EventListener, "_calculate_handler")
    @patch.object(EventListener, '_handle_nack_exception', autospec=True)
    def test_handle_message_nack(self, mock_nack, mock_calculate_handler, event_listener):
        routing_key = "routing_key"

        mock_channel = MagicMock()
        mock_method = MagicMock()
        mock_method.delivery_tag = 1
        mock_method.routing_key = routing_key
        event = b'event'

        # Let the handler return a nack exception when handling the event
        handler_mock = mock_calculate_handler.return_value
        handler_mock.handle_event.side_effect = NackException("error")
        event_listener.handle_message(mock_channel, mock_method, None, event)

        # Check if calculate handler method has been called
        assert mock_calculate_handler.call_count == 1
        assert mock_calculate_handler.call_args[0][0] == routing_key

        # Check if handle event of handler has been called
        handler_mock = mock_calculate_handler()
        assert handler_mock.handle_event.call_count == 1
        assert handler_mock.handle_event.call_args[0][0] == event

        # Check if message has been sent to queue
        assert mock_channel.basic_ack.call_count == 0
        assert mock_nack.call_count == 1
        assert mock_nack.call_args[0][1].message == "error"
        assert mock_nack.call_args[0][2] == mock_channel
        assert mock_nack.call_args[0][3] == 1


class AbstractBaseHandler(ABC):
    @pytest.mark.parametrize(
        "actual_amount,expected_amount",
        [(1, 1), (2, -1)]
    )
    def test_get_fragment(self, actual_amount, expected_amount, handler):
        result_dict = {"TotalNrOfResults": actual_amount}
        mh_client_mock = handler.mh_client
        mh_client_mock.get_fragment.return_value = result_dict

        key_values = [("key", "value")]
        assert handler._get_fragment(key_values, expected_amount) == result_dict
        assert mh_client_mock.get_fragment.call_count == 1
        assert mh_client_mock.get_fragment.call_args[0][0] == key_values

    def test_get_fragment_nr_of_results_mismatch(self, handler):
        result_dict = {"TotalNrOfResults": 1}
        mh_client_mock = handler.mh_client
        mh_client_mock.get_fragment.return_value = result_dict

        key_values = [("key", "value")]
        with pytest.raises(NackException):
            handler._get_fragment(key_values, 2)
        assert mh_client_mock.get_fragment.call_count == 1
        assert mh_client_mock.get_fragment.call_args[0][0] == key_values

    def test_get_fragment_http_error(self, http_error, handler):
        mh_client_mock = handler.mh_client
        # Raise a HTTP Error when calling method
        handler.mh_client.get_fragment.side_effect = http_error

        key_values = [("key", "value")]
        with pytest.raises(NackException) as error:
            handler._get_fragment(key_values)
        assert not error.value.requeue
        assert error.value.kwargs["error"] == http_error
        assert error.value.kwargs["error_response"] == http_error.response.text
        assert error.value.kwargs["query_key_values"] == key_values

        assert mh_client_mock.get_fragment.call_count == 1
        assert mh_client_mock.get_fragment.call_args[0][0] == key_values


class TestEventLinkedHandler(AbstractBaseHandler):
    @pytest.fixture
    def handler(self):
        """ Creates an essence linked handler with a mocked logger, rabbit client,
        MH client and PID Service.
        """
        return EssenceLinkedHandler(MagicMock(), MagicMock(), MagicMock(), "routing_key", MagicMock())

    def test_generate_get_metadata_request_xml(self, handler):
        # Create getMetadataRequest XML
        xml = handler._generate_get_metadata_request_xml(
            datetime.now().isoformat(), "cor", "media"
        )

        # Load in XML schema
        schema = etree.XMLSchema(file=construct_filename("essenceEvents.xsd"))

        # Parse getMetadataRequest XML as tree
        tree = etree.parse(BytesIO(xml.encode("utf-8")))

        # Assert validness according to schema
        is_xml_valid = schema.validate(tree)
        assert is_xml_valid

    def test_parse_event(self, handler):
        event = load_resource("essenceLinkedEvent.xml")
        event = handler._parse_event(event)
        assert event is not None
        assert event.file == "file.mxf"

    @patch.object(EssenceEvent, '__init__', side_effect=InvalidEventException("error"))
    def test_parse_event_invalid(self, init_mock, handler):
        with pytest.raises(NackException) as error:
            handler._parse_event("")
        assert not error.value.requeue
        assert error.value.kwargs["error"] == init_mock.side_effect

    @patch.object(EssenceLinkedHandler, "_parse_event")
    @patch.object(EssenceLinkedHandler, "_get_fragment")
    @patch.object(EssenceLinkedHandler, "_parse_umid")
    @patch.object(EssenceLinkedHandler, "_get_pid")
    @patch.object(EssenceLinkedHandler, "_create_fragment")
    @patch.object(EssenceLinkedHandler, "_parse_fragment_id")
    @patch.object(EssenceLinkedHandler, "_add_metadata", return_value=True)
    @patch.object(EssenceLinkedHandler, "_generate_get_metadata_request_xml", return_value="xml")
    def test_handle_event_update(
        self,
        mock_generate_get_metadata_request_xml,
        mock_add_metadata,
        mock_parse_fragment_id,
        mock_create_fragment,
        mock_get_pid,
        mock_parse_umid,
        mock_get_fragment,
        mock_parse_event,
        handler
    ):
        handler.handle_event("irrelevant")
        assert mock_parse_event.call_count == 1
        assert mock_get_fragment.call_count == 2

        # First call
        args = ([("s3_object_key", mock_parse_event().file), ("IsFragment", 0)], 1)
        assert mock_get_fragment.call_args_list[0][0] == args

        # Second call
        args = ([("dc_identifier_localid", mock_parse_event().media_id)], 0)
        assert mock_get_fragment.call_args_list[1][0] == args

        assert mock_parse_umid.call_count == 1
        assert mock_create_fragment.call_count == 1
        assert mock_parse_fragment_id.call_count == 1
        assert mock_get_pid.call_count == 1
        assert mock_add_metadata.call_count == 1
        assert mock_generate_get_metadata_request_xml.call_count == 1
        assert handler.rabbit_client.send_message.call_count == 1
        assert handler.rabbit_client.send_message.call_args[0][0] == "xml"
        assert handler.rabbit_client.send_message.call_args[0][1] == handler.routing_key

    def test_parse_umid(self, handler):
        object_id = "object id"
        fragment = {"MediaDataList": [{"Internal": {"MediaObjectId": object_id}}]}
        assert handler._parse_umid(fragment) == object_id

    def test_parse_umid_key_error(self, handler):
        object_id = "object id"
        fragment = {"wrong": object_id}
        with pytest.raises(NackException) as error:
            handler._parse_umid(fragment)
        assert not error.value.requeue

    def test_get_pid(self, handler):
        pid_service_mock = handler.pid_service
        pid_service_mock.get_pid.return_value = "pid"

        assert handler._get_pid() == "pid"

    def test_get_pid_none(self, handler):
        pid_service_mock = handler.pid_service
        pid_service_mock.get_pid.return_value = None

        with pytest.raises(NackException) as error:
            handler._get_pid()
        assert error.value.requeue

    def test_create_fragment(self, handler):
        mh_client_mock = handler.mh_client
        fragment_id = "fragment id"
        umid = "umid"
        fragment_response = {"fragment_id": fragment_id}
        mh_client_mock.create_fragment.return_value = fragment_response

        assert handler._create_fragment(umid) == fragment_response
        assert mh_client_mock.create_fragment.call_count == 1
        assert mh_client_mock.create_fragment.call_args[0][0] == umid

    def test_create_fragment_http_error(self, http_error, handler):
        mh_client_mock = handler.mh_client
        umid = "umid"

        # Raise a HTTP Error when calling method
        mh_client_mock.create_fragment.side_effect = http_error

        with pytest.raises(NackException) as error:
            handler._create_fragment(umid)
        assert not error.value.requeue
        assert error.value.kwargs["error"] == http_error
        assert error.value.kwargs["error_response"] == http_error.response.text
        assert error.value.kwargs["umid"] == umid

        assert mh_client_mock.create_fragment.call_count == 1
        assert mh_client_mock.create_fragment.call_args[0][0] == umid

    def test_create_fragment_requests_exception(self, handler):
        mh_client_mock = handler.mh_client
        umid = "umid"

        # Raise a HTTP Error when calling method
        mh_client_mock.create_fragment.side_effect = RequestException

        with pytest.raises(NackException) as error:
            handler._create_fragment(umid)
        assert error.value.requeue
        assert mh_client_mock.create_fragment.call_count == 1
        assert mh_client_mock.create_fragment.call_args[0][0] == umid

    def test_parse_fragment_id(self, handler):
        fragment_id = "fragment id"
        response = {"Internal": {"FragmentId": fragment_id}}
        assert handler._parse_fragment_id(response) == fragment_id

    def test_parse_fragment_id_key_error(self, handler):
        fragment_id = "fragment id"
        response = {"wrong": fragment_id}
        with pytest.raises(NackException) as error:
            handler._parse_fragment_id(response)
        assert not error.value.requeue

    def test_add_metadata(self, handler):
        mh_client_mock = handler.mh_client
        fragment_id = "fragment id"
        media_id = "media id"
        pid = "pid"

        # Return True, which means the action was successful
        mh_client_mock.add_metadata_to_fragment.return_value = True

        assert handler._add_metadata(fragment_id, media_id, pid) is None
        assert mh_client_mock.add_metadata_to_fragment.call_count == 1
        assert mh_client_mock.add_metadata_to_fragment.call_args[0][0] == fragment_id
        assert mh_client_mock.add_metadata_to_fragment.call_args[0][1] == media_id
        assert mh_client_mock.add_metadata_to_fragment.call_args[0][2] == pid

    def test_add_metadata_http_error(self, http_error, handler):
        mh_client_mock = handler.mh_client
        fragment_id = "fragment id"
        media_id = "media id"
        pid = "pid"

        # Raise a HTTP Error when calling method
        http_error.status_code = 400
        mh_client_mock.add_metadata_to_fragment.side_effect = http_error

        with pytest.raises(NackException) as error:
            handler._add_metadata(fragment_id, media_id, pid)
        assert not error.value.requeue
        assert error.value.kwargs["error"] == http_error
        assert error.value.kwargs["error_response"] == http_error.response.text
        assert error.value.kwargs["fragment_id"] == fragment_id
        assert error.value.kwargs["media_id"] == media_id

        assert mh_client_mock.add_metadata_to_fragment.call_count == 1
        assert mh_client_mock.add_metadata_to_fragment.call_args[0][0] == fragment_id
        assert mh_client_mock.add_metadata_to_fragment.call_args[0][1] == media_id
        assert mh_client_mock.add_metadata_to_fragment.call_args[0][2] == pid

    def test_add_metadata_false(self, handler):
        mh_client_mock = handler.mh_client
        fragment_id = "fragment id"
        media_id = "media id"
        pid = "pid"

        mh_client_mock.add_metadata_to_fragment.return_value = False

        with pytest.raises(NackException) as error:
            handler._add_metadata(fragment_id, media_id, pid)
        assert not error.value.requeue
        assert error.value.kwargs.get("error") is None


class AbstractTestDeleteFragmentHandler(AbstractBaseHandler):
    @patch.object(EssenceEvent, '__init__', side_effect=InvalidEventException("error"))
    def test_parse_event_invalid(self, init_mock, handler):
        with pytest.raises(NackException) as error:
            handler._parse_event("")
        assert not error.value.requeue
        assert error.value.kwargs["error"] == init_mock.side_effect


    def test_delete_fragment(self, handler):
        mh_client_mock = handler.mh_client
        fragment_id = "fragment id"

        # Return True, which means the action was successful
        mh_client_mock.delete_fragment.return_value = True

        assert handler._delete_fragment(fragment_id)
        assert mh_client_mock.delete_fragment.call_count == 1
        assert mh_client_mock.delete_fragment.call_args[0][0] == fragment_id

    def test_delete_fragment_http_error(self, http_error, handler):
        mh_client_mock = handler.mh_client
        fragment_id = "fragment id"

        # Raise a HTTP Error when calling method
        mh_client_mock.delete_fragment.side_effect = http_error

        with pytest.raises(NackException) as error:
            handler._delete_fragment(fragment_id)
        assert not error.value.requeue
        assert error.value.kwargs["error"] == http_error
        assert error.value.kwargs["error_response"] == http_error.response.text
        assert error.value.kwargs["fragment_id"] == fragment_id

        assert mh_client_mock.delete_fragment.call_count == 1
        assert mh_client_mock.delete_fragment.call_args[0][0] == fragment_id

    def test_delete_fragment_request_exception(self, handler):
        mh_client_mock = handler.mh_client
        fragment_id = "fragment id"

        # Raise a HTTP Error when calling method
        mh_client_mock.delete_fragment.side_effect = RequestException

        with pytest.raises(NackException) as error:
            handler._delete_fragment(fragment_id)
        assert error.value.requeue
        assert mh_client_mock.delete_fragment.call_count == 1
        assert mh_client_mock.delete_fragment.call_args[0][0] == fragment_id


class TestEventUnlinkedHandler(AbstractTestDeleteFragmentHandler):
    @pytest.fixture
    def handler(self):
        """ Creates an essence unlinked handler with a mocked logger and MH client """
        return EssenceUnlinkedHandler(MagicMock(), MagicMock())

    def test_parse_event(self, handler):
        event = load_resource("essenceUnlinkedEvent.xml")
        event = handler._parse_event(event)
        assert event is not None
        assert event.media_id == "media id"

    @patch.object(EssenceUnlinkedHandler, "_parse_event")
    @patch.object(EssenceUnlinkedHandler, "_get_fragment", return_value={"TotalNrOfResults": 0})
    @patch.object(EssenceUnlinkedHandler, "_parse_fragment_ids")
    @patch.object(EssenceUnlinkedHandler, "_delete_fragment",  return_value=False)
    def test_handle_event_delete_false(
        self,
        mock_delete_fragment,
        mock_parse_fragment_ids,
        mock_get_fragment,
        mock_parse_event,
        handler
    ):
        """ If the delete fragment call to MH returns a status code in the 200 range
        but not a 204, it will be seen as unsuccessful. In this case it should
        stop the handling flow and send a nack to rabbit
        """
        with pytest.raises(NackException) as error:
            handler.handle_event("irrelevant")
        assert not error.value.requeue
        assert mock_parse_event.call_count == 1
        args = ([("dc_identifier_localid", mock_parse_event().media_id),],)
        assert mock_get_fragment.call_args[0] == args
        assert mock_get_fragment.call_count == 1


class TestObjectDeletedHandler(AbstractTestDeleteFragmentHandler):
    @pytest.fixture
    def handler(self):
        """ Creates an object deleted handler with a mocked logger and MH client """
        return ObjectDeletedHandler(MagicMock(), MagicMock())

    def test_parse_event(self, handler):
        event = load_resource("objectDeletedEvent.xml")
        event = handler._parse_event(event)
        assert event is not None
        assert event.media_id == "media id"

    @patch.object(ObjectDeletedHandler, "_parse_event")
    @patch.object(ObjectDeletedHandler, "_get_fragment", return_value={"TotalNrOfResults": 0})
    @patch.object(ObjectDeletedHandler, "_parse_fragment_ids")
    @patch.object(ObjectDeletedHandler, "_delete_fragment",  return_value=False)
    def test_handle_event_delete_false(
        self,
        mock_delete_fragment,
        mock_parse_fragment_ids,
        mock_get_fragment,
        mock_parse_event,
        handler
    ):
        """ If the delete fragment call to MH returns a status code in the 200 range
        but not a 204, it will be seen as unsuccessful. In this case it should
        stop the handling flow and send a nack to rabbit
        """
        with pytest.raises(NackException) as error:
            handler.handle_event("irrelevant")
        assert not error.value.requeue
        assert mock_parse_event.call_count == 1
        args = ([("dc_identifier_localid", mock_parse_event().media_id),],)
        assert mock_get_fragment.call_args[0] == args
        assert mock_get_fragment.call_count == 1


class TestUnknownRoutingKeyHandler:
    def test_handle_event(self, caplog):
        unknown_routing_key = "routing_key"
        message = "message"
        handler = UnknownRoutingKeyHandler(unknown_routing_key)
        with pytest.raises(NackException) as error:
            handler.handle_event(message)
        assert not error.value.requeue
        assert unknown_routing_key in error.value.message
        assert error.value.kwargs["incoming_message"] == message
