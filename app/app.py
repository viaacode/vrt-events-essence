#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Tuple

from pika.exceptions import AMQPConnectionError
from requests.exceptions import RequestException
from lxml import etree

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
from mediahaven import MediaHaven
from mediahaven.resources.base_resource import MediaHavenPageObject
from mediahaven.mediahaven import MediaHavenException, ContentType
from mediahaven.oauth2 import ROPCGrant, RequestTokenError
from app.services.pid import PIDService
from app.helpers.retry import retry, RetryException


NAMESPACE_MHS = "https://zeticon.mediahaven.com/metadata/20.1/mhs/"
NSMAP = {"mhs": NAMESPACE_MHS}
SLEEP_TIME = 0.7


class NackException(Exception):
    """Exception raised when there is a situation in which handling
    of the event should be stopped.
    """

    def __init__(self, message, requeue=False, **kwargs):
        self.message = message
        self.requeue = requeue
        self.kwargs = kwargs


class BaseHandler(ABC):
    """Abstract base class that will handle an incoming event"""

    def __init__(self):
        pass

    @abstractmethod
    def handle_event(self, message: str):
        pass

    @abstractmethod
    def _parse_event(self, message: str) -> EssenceEvent:
        pass

    def _create_query(self, query_key_values: List[Tuple[str, object]]):
        return " ".join([f'+({k_v[0]}: "{k_v[1]}")' for k_v in query_key_values])

    def _get_fragment(
        self, query_key_values: List[Tuple[str, object]], expected_amount: int = -1
    ) -> MediaHavenPageObject:
        """Gets a fragment based on a query given a list of keys and values.

        Also checks if the actual amount of results is what we expect. If the expected
        amount is -1, the check will be skipped.

        Arguments:
            query_key_values -- A list of key-value tuples.
            expected_amount -- Expected amount of results. default: -1 (no check).
        Raises:
            NackException:
                If the expected amount is different than the actual amount.
                When an MediaHavenException is returned when querying MH.
        """
        try:
            self.log.debug(f"Retrieve fragment with {query_key_values}")
            response = self.mh_client.records.search(
                q=self._create_query(query_key_values)
            )
            number_of_results = response.total_nr_of_results
            if expected_amount > -1 and number_of_results != expected_amount:
                raise NackException(
                    f"Expected {expected_amount} result(s) with {query_key_values}, found {number_of_results} result(s)",
                    query_key_values=query_key_values,
                )
            time.sleep(SLEEP_TIME)
        except MediaHavenException as error:
            raise NackException(
                f"Unable to retrieve fragment for {query_key_values}",
                error=error,
                error_response=str(error),
                query_key_values=query_key_values,
            )
        except RequestException as error:
            raise NackException(
                "Unable to connect to MediaHaven", error=error, requeue=True
            )
        return response


class EssenceLinkedHandler(BaseHandler):
    """Class that will handle an incoming essence linked event"""

    def __init__(self, logger, mh_client, rabbit_client, routing_key, pid_service):
        self.log = logger
        self.mh_client = mh_client
        self.rabbit_client = rabbit_client
        self.routing_key = routing_key
        self.pid_service = pid_service

    def _construct_metadata(self, media_id: str, pid: str, ie_type: str) -> str:
        """Create the sidecar XML to upload the metadata.

        Returns:
            str -- The metadata sidecar XML.
        """
        root = etree.Element(f"{{{NAMESPACE_MHS}}}Sidecar", nsmap=NSMAP, version="20.1")
        # /Dynamic
        dynamic = etree.SubElement(root, f"{{{NAMESPACE_MHS}}}Dynamic")
        # /Dynamic/dc_identifier_localid
        etree.SubElement(dynamic, "dc_identifier_localid").text = media_id
        # /Dynamic/PID
        etree.SubElement(dynamic, "PID").text = pid
        # /Dynamic/dc_identifier_localids
        local_ids = etree.SubElement(dynamic, "dc_identifier_localids")
        # /Dynamic/dc_identifier_localids/MEDIA_ID
        etree.SubElement(local_ids, "MEDIA_ID").text = media_id
        # /Dynamic/object_level
        etree.SubElement(dynamic, "object_level").text = "ie"
        # /Dynamic/object_use
        etree.SubElement(dynamic, "object_use").text = "archive_master"
        # /Dynamic/ie_type
        etree.SubElement(dynamic, "ie_type").text = ie_type

        return etree.tostring(
            root, pretty_print=False, encoding="UTF-8", xml_declaration=True
        ).decode("utf-8")

    def _generate_get_metadata_request_xml(
        self, timestamp: datetime, correlation_id: str, media_id: str
    ) -> str:
        """Generates an xml for the getMetaDataRequest event.

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
            "mediaId": media_id,
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
            "Start handling essence linked event", essence_linked_event=message
        )

        # Parse event
        event = self._parse_event(message)

        # We do not handle original videos
        if event.s3_bucket == "original-video":
            self.log.info(
                f"Skipped {event.file} because it arrived in the {event.s3_bucket} bucket."
            )
            return

        filename = event.file
        media_id = event.media_id

        # Get the main fragment
        fragment = self._get_fragment(
            [("s3_object_key", filename), ("IsFragment", 0)], 1
        )

        # Check if there are no fragments with the media ID
        self._get_fragment([("dc_identifier_localid", media_id)], 0)

        # Parse the umid from the MediaHaven object
        umid = self._parse_umid(fragment)

        # Parse the intellectual entity type from the MediaHaven object
        ie_type = self._parse_ie_type(fragment)

        # Get a pid to use for the new fragment
        pid = self._get_pid()

        # Create fragment for main fragment
        create_fragment_response = self._create_fragment(
            umid, fragment[0].Descriptive.Title
        )

        # Parse the fragmentId from the response of the newly created fragment.
        fragment_id = self._parse_fragment_id(create_fragment_response)

        # Add Media ID, PID and information for DEEWEE to the newly created fragment
        self._add_metadata(fragment_id, media_id, pid, ie_type)

        # Build metadata request XML
        xml = self._generate_get_metadata_request_xml(
            datetime.now().astimezone().isoformat(),  # Local timezone-aware timestamp
            media_id,  # Correlation_id is the media_id
            media_id,
        )

        # Send metadata request to the queue
        self.rabbit_client.send_message(xml, self.routing_key)
        self.log.info(
            f"getMetadataRequest sent for fragment id: {fragment_id}",
            fragment_id=fragment_id,
            media_id=media_id,
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

    def _parse_umid(self, fragment: MediaHavenPageObject) -> str:
        try:
            umid = fragment[0].Internal.MediaObjectId
        except AttributeError as error:
            raise NackException(
                "MediaObjectId not found in the MediaHaven object",
                error=error,
                fragment=fragment[0],
            )
        return umid

    def _parse_ie_type(self, fragment: MediaHavenPageObject) -> str:
        try:
            ie_type = fragment[0].Administrative.Type
        except AttributeError:
            return None
        return ie_type

    def _create_fragment(self, umid: str, title: str) -> dict:
        try:
            self.log.debug(f"Creating fragment for object with umid: {umid}")
            create_fragment_response = self.mh_client.records.create_fragment(
                umid, title, start_frames=0, end_frames=0
            )
            time.sleep(SLEEP_TIME)
        except MediaHavenException as error:
            raise NackException(
                f"Unable to create a fragment for umid: {umid}",
                error=error,
                error_response=str(error),
                umid=umid,
            )
        except RequestException as error:
            raise NackException(
                "Unable to connect to MediaHaven", error=error, umid=umid, requeue=True
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
        """Fetches a PID from the PID service.

        Raises:
            NackException -- If unable to get a PID

        Returns:
            str -- The generated PID
        """

        pid = self.pid_service.get_pid()
        if not pid:
            raise NackException("Unable to get a pid", requeue=True)
        return pid

    @retry(RetryException)
    def _add_metadata_to_fragment(
        self, fragment_id: str, sidecar: str, media_id: str, pid: str
    ) -> bool:
        """Utility method for adding the metadata to a fragment.

        Returns: True if successful."""
        try:
            return self.mh_client.records.update(
                fragment_id,
                metadata=sidecar,
                metadata_content_type=ContentType.XML.value,
                reason=f"essenceLinked: add mediaID {media_id} and PID {pid} to fragment",
            )
            time.sleep(SLEEP_TIME)
        except MediaHavenException as error:
            if error.status_code in (403, 404):
                raise RetryException(
                    f"Unable to update metadata for fragment_id: {fragment_id} with status code: {error.status_code}",
                )
            else:
                raise error

    def _add_metadata(self, fragment_id: str, media_id: str, pid: str, ie_type: str):
        """Adds the media ID, PID and information for DEEWEE as metadata to the fragment.

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
            ie_type -- The type of intellectual entity: 'audio' or 'video'

        Raises:
            NackException -- When we were unable to add the metadata (see above).
        """

        # Create the payload
        sidecar = self._construct_metadata(media_id, pid, ie_type)

        try:
            result = self._add_metadata_to_fragment(fragment_id, sidecar, media_id, pid)
        except MediaHavenException as error:
            raise NackException(
                f"Unable to update the metadata for fragment id: {fragment_id} and media id: {media_id}",
                error=error,
                error_response=str(error),
                fragment_id=fragment_id,
                media_id=media_id,
            )
        except RequestException as error:
            raise NackException(
                "Unable to connect to MediaHaven",
                error=error,
                fragment_id=fragment_id,
                requeue=True,
            )
        if not result:
            raise NackException(
                f"Unable to update the metadata for fragment id: {fragment_id} and media id: {media_id}",
                fragment_id=fragment_id,
                media_id=media_id,
            )


class DeleteFragmentHandler(BaseHandler):
    """Abstract class that will handle an incoming event that will result in deleting
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

        # Get all items for the media id (this will include the fragment + collaterals)
        response = self._get_fragment([("dc_identifier_localid", media_id)])

        if not response.total_nr_of_results:
            raise NackException(
                f"No fragments found for media id: {media_id}",
                media_id=media_id,
            )

        # Delete the fragment for the fragment_id
        for record in response:
            fragment_id = record.Internal.FragmentId

            self.log.debug(
                f"Deleting fragment for object with fragment id: {fragment_id}",
                fragment_id=fragment_id,
                media_id=media_id,
            )
            result = self._delete_fragment(fragment_id, event.media_id, event.root_tag)
            if not result:
                raise NackException(
                    f"Unable to delete the fragment for fragment id: {fragment_id}",
                    fragment_id=fragment_id,
                    media_id=media_id,
                )

        self.log.info(
            f"Successfully deleted {response.total_nr_of_results} item(s) with media id: {media_id}"
        )

    def _delete_fragment(
        self, fragment_id: str, reason: str = "", event_type: str = ""
    ) -> bool:
        try:
            result = self.mh_client.records.delete(fragment_id)
            time.sleep(SLEEP_TIME)
        except MediaHavenException as error:
            raise NackException(
                f"Unable to delete a fragment for fragment_id: {fragment_id}",
                error=error,
                error_response=str(error),
                fragment_id=fragment_id,
            )
        except RequestException as error:
            raise NackException(
                "Unable to connect to MediaHaven",
                error=error,
                fragment_id=fragment_id,
                requeue=True,
            )
        return result


class EssenceUnlinkedHandler(DeleteFragmentHandler):
    """Class that will handle an incoming essence unlinked event"""

    def _parse_event(self, message: str) -> EssenceUnlinkedEvent:
        self.log.info(
            "Start handling essence unlinked event", essence_unlinked_event=message
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
    """Class that will handle an incoming object deleted event"""

    def _parse_event(self, message: str) -> ObjectDeletedEvent:
        self.log.info(
            "Start handling object deleted event", object_deleted_event=message
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
    """Class that will handle an incoming event with an unknown routing key"""

    def __init__(self, routing_key: str):
        self.routing_key = routing_key

    def handle_event(self, message: str):
        raise NackException(
            f"Unknown routing key: {self.routing_key}", incoming_message=message
        )


class EventListener:
    def __init__(self):
        configParser = ConfigParser()
        self.config = configParser.app_cfg
        self.log = logging.get_logger(__name__, config=configParser)

        grant = ROPCGrant(
            self.config["mediahaven"]["host"],
            self.config["mediahaven"]["client_id"],
            self.config["mediahaven"]["client_secret"],
        )
        try:
            grant.request_token(
                self.config["mediahaven"]["username"],
                self.config["mediahaven"]["password"],
            )
        except RequestTokenError as error:
            self.log.error("Requesting MH token has failed.")
            raise error

        self.mh_client = MediaHaven(self.config["mediahaven"]["host"], grant)

        try:
            self.rabbit_client = RabbitClient()
        except AMQPConnectionError as error:
            self.log.error("Connection to RabbitMQ failed.")
            raise error
        self.pid_service = PIDService(self.config["pid-service"]["URL"])
        self.essence_linked_rk = self.config["rabbitmq"]["essence_linked_routing_key"]
        self.essence_unlinked_rk = self.config["rabbitmq"][
            "essence_unlinked_routing_key"
        ]
        self.object_deleted_rk = self.config["rabbitmq"]["object_deleted_routing_key"]
        self.get_metadata_rk = self.config["rabbitmq"]["get_metadata_routing_key"]

    def _handle_nack_exception(self, nack_exception, channel, delivery_tag):
        """Log an error and send a nack to rabbit"""
        self.log.error(nack_exception.message, **nack_exception.kwargs)
        if nack_exception.requeue:
            time.sleep(10)
        channel.basic_nack(delivery_tag=delivery_tag, requeue=nack_exception.requeue)

    def _calculate_handler(self, routing_key: str):
        """Return the correct handler given the routing key"""
        event_type = routing_key.split(".")[-1]
        if event_type == self.essence_linked_rk:
            return EssenceLinkedHandler(
                self.log,
                self.mh_client,
                self.rabbit_client,
                self.get_metadata_rk,
                self.pid_service,
            )
        if event_type == self.essence_unlinked_rk:
            return EssenceUnlinkedHandler(self.log, self.mh_client)
        if event_type == self.object_deleted_rk:
            return ObjectDeletedHandler(self.log, self.mh_client)
        return UnknownRoutingKeyHandler(event_type)

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
        except NackException as e:
            self._handle_nack_exception(e, channel, method.delivery_tag)
            return
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        # Start listening for incoming messages
        self.log.info("Start to listen for incoming essence events...")
        self.rabbit_client.listen(self.handle_message)
