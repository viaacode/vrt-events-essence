#!/usr/bin/env python
# -*- coding: utf-8 -*-

import functools
import time
from datetime import datetime

from lxml.etree import XMLSyntaxError
from pika.exceptions import AMQPConnectionError
from requests.exceptions import HTTPError, RequestException

from viaa.configuration import ConfigParser
from viaa.observability import logging
from app.helpers.events_parser import EssenceLinkedEvent, EssenceUnlinkedEvent
from app.helpers.xml_builder_vrt import XMLBuilderVRT
from app.services.rabbit import RabbitClient
from app.services.mediahaven import MediahavenClient


class RetryException(Exception):
    """ Exception raised when an action needs to be retried
    in combination with _retry decorator"""
    pass


class NackException(Exception):
    """ Exception raised when there is a situation in which handling
    of the event should be stopped.
    """
    def __init__(self, message, requeue=False, **kwargs):
        self.message = message
        self.requeue = requeue
        self.kwargs = kwargs


class EssenceLinkedHandler:
    """ Class that will handle an incoming essence linked event """
    def __init__(self, logger, mh_client, rabbit_client, routing_key):
        self.log = logger
        self.mh_client = mh_client
        self.rabbit_client = rabbit_client
        self.routing_key = routing_key

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
            NackException -- When something happens that stops the handling.
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
        fragment = self._get_fragment(filename)

        # Parse the umid from the MediaHaven object
        umid = self._parse_umid(fragment)

        # Create fragment for main fragment
        create_fragment_response = self._create_fragment(umid)

        # Parse the fragmentId from the response of the newly created fragment.
        fragment_id = self._parse_fragment_id(create_fragment_response)

        # Add Media_id to the newly created fragment
        result = self._add_metadata(fragment_id, media_id)
        if not result:
            raise NackException(
                f"Unable to update the metadata for fragment id: {fragment_id} and media id: {media_id}",
                fragment_id=fragment_id,
                media_id=media_id
            )

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
        except XMLSyntaxError as error:
            raise NackException(
                "Unable to parse the incoming essence linked event",
                error=error,
                essence_linked_event=message,
            )
        return event

    def _get_fragment(self, filename: str) -> dict:
        try:
            self.log.debug(f"Retrieve fragment with s3 object key: {filename}")
            fragment = self.mh_client.get_fragment('s3_object_key', filename)
        except HTTPError as error:
            raise NackException(
                f"Unable to retrieve MediaHaven object for s3_object_key: {filename}",
                error=error,
                s3_object_key=filename,
            )
        return fragment

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

    def _retry(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            delay = 1
            backoff = 2
            number_of_tries = 5
            tries = 5
            while tries:
                tries -= 1
                try:
                    return func(self, *args, **kwargs)
                except RetryException as error:
                    self.log.debug(f"{error}. Retrying in {delay} seconds.", try_count = number_of_tries - tries)
                    time.sleep(delay)
                    delay *= backoff
            return False
        return wrapper

    @_retry
    def _add_metadata(self, fragment_id: str, media_id: str) -> bool:
        try:
            result = self.mh_client.add_metadata_to_fragment(fragment_id, media_id)
        except HTTPError as error:
            if error.response.status_code == 404:
                raise RetryException(f"Unable to update metadata for fragment_id: {fragment_id}")
            else:
                raise NackException(
                    f"Unable to add MediaID metadata for fragment_id: {fragment_id}",
                    error=error,
                    fragment_id=fragment_id,
                    media_id=media_id,
                )
        return result


class EssenceUnlinkedHandler:
    """ Class that will handle an incoming essence unlinked event """
    def __init__(self, logger, mh_client):
        self.log = logger
        self.mh_client = mh_client

    def handle_event(self, message: str):
        """Handle an incoming essence unlinked event.

        First we parse the XML message into a EssenceUnlinkedEvent.
        Then we search in mediahaven for the fragment based on
        the dc_identifier_localid (=mediaId in the essence unlinked event).
        Then we delete the fragment for the fragment id.


        Arguments:
            message {str} -- Essence unlinked event XML message.

        Raises:
            NackException -- When something happens that stops the handling.
        """

        self.log.info(
            'Start handling essence unlinked event',
            essence_unlinked_event=message
        )

        # Parse event
        event = self._parse_event(message)

        media_id = event.media_id

        # Get the fragment based on the media_id
        fragment = self._get_fragment(media_id)

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

    def _parse_event(self, message: str) -> EssenceUnlinkedEvent:
        try:
            event = EssenceUnlinkedEvent(message)
        except XMLSyntaxError as error:
            raise NackException(
                "Unable to parse the incoming essence unlinked event",
                error=error,
                essence_unlinked_event=message,
            )
        return event

    def _get_fragment(self, media_id: str) -> dict:
        try:
            self.log.debug(f"Retrieve fragment with dc_identifier_localid: {media_id}")
            fragment = self.mh_client.get_fragment('dc_identifier_localid', media_id)
        except HTTPError as error:
            raise NackException(
                f"Unable to retrieve fragment for dc_identifier_localid: {media_id}",
                error=error,
                dc_identifier_localid=media_id,
            )
        return fragment

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

    def handle_message(self, channel, method, properties, body):
        """Main method that will handle the incoming messages.

        Based on the routing key it will process the message accordingly.
        There are three types of events this app will process:
        essenceLinked, essenceUnLinked and objectDeleted.
        """
        routing_key = method.routing_key
        self.log.info(
            f"Incoming message with routing key: {routing_key}",
            incoming_message=body,
        )
        if routing_key == self.essence_linked_rk:
            handler = EssenceLinkedHandler(
                self.log,
                self.mh_client,
                self.rabbit_client,
                self.get_metadata_rk
            )
            try:
                handler.handle_event(body)
            except(NackException) as e:
                self._handle_nack_exception(e, channel, method.delivery_tag)
        elif routing_key == self.essence_unlinked_rk:
            handler = EssenceUnlinkedHandler(self.log, self.mh_client)
            try:
                handler.handle_event(body)
            except(NackException) as e:
                self._handle_nack_exception(e, channel, method.delivery_tag)
        elif routing_key == self.object_deleted_rk:
            # TODO process deleted
            pass
        else:
            self.log.warning(
                f"Unknown routing key: {routing_key}",
                incoming_message=body,
            )
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        # Start listening for incoming messages
        self.log.info("Start to listen for incoming essence events...")
        self.rabbit_client.listen(self.handle_message)
