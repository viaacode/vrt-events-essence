#!/usr/bin/env python
# -*- coding: utf-8 -*-

import uuid
from datetime import datetime

from pika.exceptions import AMQPConnectionError
from requests.exceptions import HTTPError

from viaa.configuration import ConfigParser
from viaa.observability import logging

from app.helpers.events_parser import (
    EssenceLinkedEvent,
    EssenceLinkedEventNotValidException
)
from app.helpers.xml_builder_vrt import XMLBuilderVRT
from app.services.rabbit import RabbitClient
from app.services.mediahaven import MediahavenClient


class EventListener:
    def __init__(self):
        configParser = ConfigParser()
        self.log = logging.get_logger(__name__, config=configParser)
        self.config = configParser.app_cfg
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

    def _handle_linked_event(self, message: str):
        """Handle an incoming essence linked event.

        First we parse the XML message into a EssenceLinkedEvent.
        Then we search in mediahaven for the main fragment based on
        the s3_object_key (=file in the essence linked event).
        Next we create a fragment for the main fragment.
        Then we add media_id to the newly created fragment.

        After all is successful, a getMedadataRequest XML will be sent
        to the queue.

        Arguments:
            message {str} -- Essence linked event XML message
        """
        self.log.info(
            'Start handling essence linked event',
            essence_linked_event=message
        )

        # Parse event
        try:
            event = EssenceLinkedEvent(message)
        except EssenceLinkedEventNotValidException as error:
            self.log.error(
                "Unable to parse the incoming essence linked event",
                error=error,
                message=message,
            )
            return

        filename = event.file
        media_id = event.media_id

        # Get the main fragment
        try:
            fragment = self.mh_client.get_fragment('s3_object_key', filename)
        except HTTPError as error:
            self.log.error(
                f"Unable to retrieve MediaHaven object for s3_object_key: {filename}",
                error=error,
                s3_object_key=filename,
            )
            return

        # Retrieve the umid from the MediaHaven object
        try:
            umid = fragment["MediaDataList"][0]["Internal"]["MediaObjectId"]
        except KeyError as error:
            self.log.error(
                "MediaObjectId not found in the MediaHaven object",
                error=error,
                fragment=fragment,
            )
            return

        # Create fragment for main fragment
        try:
            create_fragment_response = self.mh_client.create_fragment(umid)
        except HTTPError as error:
            self.log.error(
                f"Unable to create a fragment for umid: {umid}",
                error=error,
                umid=umid,
            )
            return

        # Retrieve the fragmentId from the response of the newly created fragment.
        try:
            fragment_id = create_fragment_response["Internal"]["FragmentId"]
        except KeyError as error:
            self.log.error(
                "fragmentId not found in the response of the create fragment call",
                create_fragment_response=create_fragment_response,
                error=error,
            )
            return

        # Add Media_id to the newly created fragment
        try:
            self.mh_client.add_metadata_to_fragment(fragment_id, media_id)
        except HTTPError as error:
            self.log.error(
                f"Unable to add MediaID metadata for umid: {umid}",
                error=error,
                umid=umid,
                media_id=media_id,
            )
            return

        # Build metadata request XML
        xml = self._generate_get_metadata_request_xml(
            datetime.now().isoformat(),
            media_id, # Correlation_id is the media_id
            media_id,
        )

        # Send metadata request to the queue
        self.rabbit_client.send_message(xml, self.get_metadata_rk)

    def handle_message(self, channel, method, properties, body):
        """Main method that will handle the incoming messages.

        Based on the routing key it will process the message accordingly.
        There are three types of events this app will process:
        essenceLinked, essenceUnLinked and objectDeleted.
        """
        routing_key = method.routing_key
        if routing_key == self.essence_linked_rk:
            self._handle_linked_event(body)
        elif routing_key == self.essence_unlinked_rk:
            # TODO process unlinked
            pass
        elif routing_key == self.object_deleted_rk:
            # TODO process deleted
            pass
        else:
            # TODO log/raise
            pass
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        # Start listening for incoming messages
        self.rabbit_client.listen(self.handle_message)
