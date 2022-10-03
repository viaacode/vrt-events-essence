#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC
from io import BytesIO
from lxml import etree

# Constants
VRT_NAMESPACE = "http://www.vrt.be/mig/viaa/api"
XPATHS = {
    "timestamp": "./p:timestamp",
    "file": "./p:file",
    "media_id": "./p:mediaId",
    "s3_bucket":"./p:s3bucket"
}


class InvalidEventException(Exception):
    def __init__(self, message, **kwargs):
        self.message = message
        self.kwargs = kwargs


class EssenceEvent(ABC):
    """Abstract class for an XML Essence Event"""
    def __init__(self, xml):
        self.xml_element = self._get_essence_event(xml)
        self.timestamp = self._get_xpath_from_event(XPATHS["timestamp"])
        self.media_id = self._get_xpath_from_event(XPATHS["media_id"])

    def _get_essence_event(self, xml: str):
        """Parse the input XML to a DOM

        Raises:
            InvalidEventException -- When the XML is not valid.
        """
        try:
            tree = etree.parse(BytesIO(xml))
        except etree.XMLSyntaxError:
            raise InvalidEventException("Event is not valid XML.")

        try:
            return tree.xpath(
                f"/p:{self.root_tag}", namespaces={"p": VRT_NAMESPACE}
            )[0]
        except IndexError:
            raise InvalidEventException(f"Event is not a '{self.root_tag}'.")

    def _get_xpath_from_event(self, xpath, optional: bool = False) -> str:
        """Parses value based on an xpath.

        Raises:
            InvalidEventException -- When XPATH is mandatory but not present
        """
        try:
            return self.xml_element.xpath(
                xpath, namespaces={"p": VRT_NAMESPACE}
            )[0].text
        except IndexError:
            if optional:
                return ""
            else:
                raise InvalidEventException(f"'{xpath}' is not present in the event.")


class EssenceLinkedEvent(EssenceEvent):
    """Convenience class for an XML Essence Linked Event"""
    root_tag = "essenceLinkedEvent"

    def __init__(self, xml):
        super().__init__(xml)
        self.file = self._get_xpath_from_event(XPATHS["file"])
        self.s3_bucket = self._get_xpath_from_event(XPATHS["s3_bucket"])


class EssenceUnlinkedEvent(EssenceEvent):
    """Convenience class for an XML Essence Unlinked Event"""
    root_tag = "essenceUnlinkedEvent"


class ObjectDeletedEvent(EssenceEvent):
    """Convenience class for an XML Object Deleted Event"""
    root_tag = "objectDeletedEvent"
