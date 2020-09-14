#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Tuple

from pika.exceptions import AMQPConnectionError
from requests.exceptions import HTTPError, RequestException

from viaa.configuration import ConfigParser
from viaa.observability import logging
from app.helpers.events_parser import (
    EssenceEvent,
    EssenceLinkedEvent,
    EssenceUnlinkedEvent,
    InvalidEventException,
    ObjectDeletedEvent,
)
from app.helpers.xml_builder_vrt import XMLBuilderVRT
from app.services.rabbit import RabbitClient
from app.services.mediahaven import MediahavenClient
from app.services.pid import PIDService


class NackException(Exception):
    """ Exception raised when there is a situation in which handling
    of the event should be stopped.
    """
    def __init__(self, message, requeue=False, **kwargs):
        self.message = message
        self.requeue = requeue
        self.kwargs = kwargs


class BaseHandler(ABC):
    """ Abstract base class that will handle an incoming event """
    def __init__(self):
        pass

    @abstractmethod
    def handle_event(self, message: str):
        pass

    @abstractmethod
    def _parse_event(self, message: str) -> EssenceEvent:
        pass

    def _get_fragment(self, query_key_values: List[Tuple[str, object]], expected_amount: int = -1) -> dict:
        """ Gets a fragment based on a query given a list of keys and values.

        Also checks if the actual amount of results is what we expect. If the expected
        amount is -1, the check will be skipped.

        Arguments:
            query_key_values -- A list of key-value tuples.
            expected_amount -- Expected amount of results. default: -1 (no check).
        Raises:
            NackException:
                If the expected amount is different than the actual amount.
                When an HTTPError is returned when querying MH.
        """
        try:
            self.log.debug(f"Retrieve fragment with {query_key_values}")
            response_dict = self.mh_client.get_fragment(query_key_values)
            number_of_results = response_dict["TotalNrOfResults"]
            if expected_amount > -1 and number_of_results != expected_amount:
                raise NackException(
                    f"Expected {expected_amount} result(s) with {query_key_values}, found {number_of_results} result(s)",
                    query_key_values=query_key_values,
                )
        except HTTPError as error:
            raise NackException(
                f"Unable to retrieve fragment for {query_key_values}",
                error=error,
                error_response=error.response.text,
                query_key_values=query_key_values
            )
        except RequestException as error:
            raise NackException(
                "Unable to connect to MediaHaven",
                error=error,
                requeue=True
            )
        return response_dict


class EssenceLinkedHandler(BaseHandler):
    """ Class that will handle an incoming essence linked event """
    def __init__(self, logger, mh_client, rabbit_client, routing_key, pid_service):
        self.log = logger
        self.mh_client = mh_client
        self.rabbit_client = rabbit_client
        self.routing_key = routing_key
        self.pid_service = pid_service

    def _generate_get_metadata_request_xml(self, timestamp: datetime, correlation_id: str, media_id: str) -> str:
        """ Generates an xml for the getMetaDataRequest event.

        This request is sent after successful handling of an essence
        linked event.

        Arguments:
            timestamp {str} -- Creation time of the event.
            correlation_id {str} -- Correlation ID.
            media_id {str} -- Media ID for the media to request the metadata.

        Returns:
            str -- The getMetadataRequest XML.
        """
        xml_data_dict = {
            "timestamp": timestamp,
            "correlationId": correlation_id,
            "mediaId": media_id
        }

        builder = XMLBuilderVRT()
        builder.build("getMetadataRequest", xml_data_dict)
        xml = builder.to_string(pretty=True)

        return xml

    def handle_event(self, message: str):
        """Handle an incoming essence linked event.

        First we parse the XML message into a EssenceLinkedEvent.
        Then we search in mediahaven for the main fragment based on
        the s3_object_key (=file in the essence linked event).
        Next we create a fragment for the main fragment.
        Then we add media_id to the newly created fragment.

        After all is successful, a getMedadataRequest XML will be sent
        to the queue.

        Arguments:
            message {str} -- Essence linked event XML message.

        Raises:
            NackError -- When something went wrong
        """
        self.log.info(
            'Start handling essence linked event',
            essence_linked_event=message
        )

        # Parse event
        event = self._parse_event(message)

        filename = event.file
        media_id = event.media_id

        # Get the main fragment
        fragment = self._get_fragment([("s3_object_key", filename), ("IsFragment", 0)], 1)

        # Check if there are no fragments with the media ID
        self._get_fragment([("dc_identifier_localid", media_id)], 0)

        # Parse the umid from the MediaHaven object
        umid = self._parse_umid(fragment)

        # Get a pid to use for the new fragment
        pid = self._get_pid()

        # Create fragment for main fragment
        create_fragment_response = self._create_fragment(umid)

        # Parse the fragmentId from the response of the newly created fragment.
        fragment_id = self._parse_fragment_id(create_fragment_response)

        # Add Media_id to the newly created fragment
        self._add_metadata(fragment_id, media_id, pid)

        # Build metadata request XML
        xml = self._generate_get_metadata_request_xml(
            datetime.now().isoformat(),
            media_id,  # Correlation_id is the media_id
            media_id,
        )

        # Send metadata request to the queue
        self.rabbit_client.send_message(xml, self.routing_key)
        self.log.info(
            f"getMetadataRequest sent for fragment id: {fragment_id}",
            fragment_id=fragment_id,
            media_id=media_id
        )

    def _parse_event(self, message: str) -> EssenceLinkedEvent:
        try:
            event = EssenceLinkedEvent(message)
        except InvalidEventException as error:
            raise NackException(
                "Unable to parse the incoming essence linked event",
                error=error,
                essence_linked_event=message,
            )
        return event

    def _parse_umid(self, fragment: dict) -> str:
        try:
            umid = fragment["MediaDataList"][0]["Internal"]["MediaObjectId"]
        except KeyError as error:
            raise NackException(
                "MediaObjectId not found in the MediaHaven object",
                error=error,
                fragment=fragment,
            )
        return umid

    def _create_fragment(self, umid: str) -> dict:
        try:
            self.log.debug(f"Creating fragment for object with umid: {umid}")
            create_fragment_response = self.mh_client.create_fragment(umid)
        except HTTPError as error:
            raise NackException(
                f"Unable to create a fragment for umid: {umid}",
                error=error,
                error_response=error.response.text,
                umid=umid,
            )
        except RequestException as error:
            raise NackException(
                "Unable to connect to MediaHaven",
                error=error,
                umid=umid,
                requeue=True
            )
        return create_fragment_response

    def _parse_fragment_id(self, create_fragment_response: dict) -> str:
        try:
            fragment_id = create_fragment_response["Internal"]["FragmentId"]
            self.log.debug(f"Fragment created with id: {fragment_id}")
        except KeyError as error:
            raise NackException(
                "fragmentId not found in the response of the create fragment call",
                create_fragment_response=create_fragment_response,
                error=error,
            )
        return fragment_id

    def _get_pid(self) -> str:
        """ Fetches a PID from the PID service.

        Raises:
            NackException -- If unable to get a PID

        Returns:
            str -- The generated PID
        """

        pid = self.pid_service.get_pid()
        if not pid:
            raise NackException("Unable to get a pid", requeue=True)
        return pid

    def _add_metadata(self, fragment_id: str, media_id: str, pid: str):
        """ Adds the media ID and PID as metadata to the fragment.

        This method is called after getting a success back from MH when sending out
        a request creating the fragment. However with how MH works, it is possible
        that the fragment has not yet been created. This result in a 404 response. We
        will retry X times with an exponential back-off in that. If unsuccessful after
        those X time we'll send a NackException.

        Another type of HTTP error will result in a NackException.

        As we actually only expect a 204 back from MH, another status code
        in the success range (e.g. 200) will be seen as unsuccessful. This
        also result in a NackException.

        Arguments:
            fragment_id -- ID of fragment to add the metadata to.
            media_id -- Media ID to add as metadata.
            pid -- PID to add as metadata.

        Raises:
            NackException -- When we were unable to add the metadata (see above).
        """

        try:
            result = self.mh_client.add_metadata_to_fragment(fragment_id, media_id, pid)
        except HTTPError as error:
            raise NackException(
                f"Unable to add MediaID metadata for fragment_id: {fragment_id}",
                error=error,
                error_response=error.response.text,
                fragment_id=fragment_id,
                media_id=media_id,
            )
        if not result:
            raise NackException(
                f"Unable to update the metadata for fragment id: {fragment_id} and media id: {media_id}",
                fragment_id=fragment_id,
                media_id=media_id
            )


class DeleteFragmentHandler(BaseHandler):
    """ Abstract class that will handle an incoming event that will result in deleting
    a fragment. Possible events: EssenceUnlinkedEvent and ObjectDeletedEvent.
     """
    def __init__(self, logger, mh_client):
        self.log = logger
        self.mh_client = mh_client

    def handle_event(self, message: str):
        """Handle an incoming event resulting in deleting the fragment.

        First we parse the XML message into its respective EssenceEvent.
        Then we search in mediahaven for the fragment based on
        the dc_identifier_localid (=mediaId in the event).
        Then we delete the fragment for the fragment id.


        Arguments:
            message {str} -- Incoming XML message.

        Raises:
            NackException -- When something happens that stops the handling.
        """

        # Parse event
        event = self._parse_event(message)

        media_id = event.media_id

        # Get the fragment based on the media_id
        fragment = self._get_fragment([("dc_identifier_localid", media_id), ("IsFragment", 1)], 1)

        # Parse the fragment_id from the MediaHaven object
        fragment_id = self._parse_fragment_id(fragment)

        # Delete the fragment for the fragment_id
        result = self._delete_fragment(fragment_id)
        if not result:
            raise NackException(
                f"Unable to delete the fragment for fragment id: {fragment_id}",
                fragment_id=fragment_id
            )

        self.log.info(f"Successfully deleted fragment with ID: {fragment_id}")

    def _parse_fragment_id(self, fragment: dict) -> str:
        try:
            fragment_id = fragment["MediaDataList"][0]["Internal"]["FragmentId"]
        except KeyError as error:
            raise NackException(
                "FragmentId not found in the MediaHaven object",
                error=error,
                fragment=fragment,
            )
        return fragment_id

    def _delete_fragment(self, fragment_id: str) -> bool:
        try:
            self.log.debug(f"Deleting fragment for object with fragment id: {fragment_id}")
            result = self.mh_client.delete_fragment(fragment_id)
        except HTTPError as error:
            raise NackException(
                f"Unable to delete a fragment for fragment_id: {fragment_id}",
                error=error,
                error_response=error.response.text,
                fragment_id=fragment_id,
            )
        except RequestException as error:
            raise NackException(
                "Unable to connect to MediaHaven",
                error=error,
                fragment_id=fragment_id,
                requeue=True
            )
        return result


class EssenceUnlinkedHandler(DeleteFragmentHandler):
    """ Class that will handle an incoming essence unlinked event """
    def _parse_event(self, message: str) -> EssenceUnlinkedEvent:
        self.log.info(
            'Start handling essence unlinked event',
            essence_unlinked_event=message
        )
        try:
            event = EssenceUnlinkedEvent(message)
        except InvalidEventException as error:
            raise NackException(
                "Unable to parse the incoming essence unlinked event",
                error=error,
                essence_unlinked_event=message,
            )
        return event


class ObjectDeletedHandler(DeleteFragmentHandler):
    """ Class that will handle an incoming object deleted event """
    def _parse_event(self, message: str) -> ObjectDeletedEvent:
        self.log.info(
            'Start handling object deleted event',
            object_deleted_event=message
        )
        try:
            event = ObjectDeletedEvent(message)
        except InvalidEventException as error:
            raise NackException(
                "Unable to parse the incoming object deleted event",
                error=error,
                object_deleted_event=message,
            )
        return event


class UnknownRoutingKeyHandler:
    """ Class that will handle an incoming event with an unknown routing key """
    def __init__(self, routing_key: str):
        self.routing_key = routing_key

    def handle_event(self, message: str):
        raise NackException(
            f"Unknown routing key: {self.routing_key}",
            incoming_message=message
        )


class EventListener:
    def __init__(self):
        configParser = ConfigParser()
        self.config = configParser.app_cfg
        self.log = logging.get_logger(__name__, config=configParser)
        self.mh_client = MediahavenClient(self.config)
        try:
            self.rabbit_client = RabbitClient()
        except AMQPConnectionError as error:
            self.log.error("Connection to RabbitMQ failed.")
            raise error
        self.pid_service = PIDService(self.config["pid-service"]["URL"])
        self.essence_linked_rk = self.config["rabbitmq"]["essence_linked_routing_key"]
        self.essence_unlinked_rk = self.config["rabbitmq"]["essence_unlinked_routing_key"]
        self.object_deleted_rk = self.config["rabbitmq"]["object_deleted_routing_key"]
        self.get_metadata_rk = self.config["rabbitmq"]["get_metadata_routing_key"]

    def _handle_nack_exception(self, nack_exception, channel, delivery_tag):
        """ Log an error and send a nack to rabbit """
        self.log.error(nack_exception.message, **nack_exception.kwargs)
        if nack_exception.requeue:
            time.sleep(10)
        channel.basic_nack(delivery_tag=delivery_tag, requeue=nack_exception.requeue)

    def _calculate_handler(self, routing_key: str):
        """ Return the correct handler given the routing key """
        if routing_key == self.essence_linked_rk:
            return EssenceLinkedHandler(
                self.log,
                self.mh_client,
                self.rabbit_client,
                self.get_metadata_rk,
                self.pid_service
            )
        if routing_key == self.essence_unlinked_rk:
            return EssenceUnlinkedHandler(self.log, self.mh_client)
        if routing_key == self.object_deleted_rk:
            return ObjectDeletedHandler(self.log, self.mh_client)
        return UnknownRoutingKeyHandler(routing_key)

    def handle_message(self, channel, method, properties, body):
        """Main method that will handle the incoming messages.

        Based on the routing key it will process the message accordingly.
        There are three types of events this app will process:
        essenceLinked, essenceUnlinked and objectDeleted.
        """
        routing_key = method.routing_key
        self.log.info(
            f"Incoming message with routing key: {routing_key}",
            incoming_message=body,
        )
        handler = self._calculate_handler(routing_key)
        try:
            handler.handle_event(body)
        except(NackException) as e:
            self._handle_nack_exception(e, channel, method.delivery_tag)
            return
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        # Start listening for incoming messages
        self.log.info("Start to listen for incoming essence events...")
        self.rabbit_client.listen(self.handle_message)
