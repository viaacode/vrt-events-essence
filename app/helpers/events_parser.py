#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import BytesIO
from lxml import etree

# Constants
VRT_NAMESPACE = "http://www.vrt.be/mig/viaa/api"
XPATHS = {
    "timestamp": "./p:timestamp",
    "file": "./p:file",
    "media_id": "./p:mediaId",
}


class EssenceEvent:
    """Abstract class for an XML Essence Event"""
    def __init__(self, xml):
        self.xml_element = self._get_essence_linked_event(xml)
        self.timestamp = self._get_xpath_from_event(XPATHS["timestamp"])
        self.media_id = self._get_xpath_from_event(XPATHS["media_id"])

    def _get_essence_linked_event(self, xml: str):
        """Parse the input XML to a DOM"""
        tree = etree.parse(BytesIO(xml))
        return tree.xpath(
            f"/p:{self.root_tag}", namespaces={"p": VRT_NAMESPACE}
        )[0]

    def _get_xpath_from_event(self, xpath) -> str:
        """Parses based on an xpath, returns empty string if absent"""
        try:
            return self.xml_element.xpath(
                xpath, namespaces={"p": VRT_NAMESPACE}
            )[0].text
        except IndexError:
            return ""


class EssenceLinkedEvent(EssenceEvent):
    """Convenience class for an XML Essence Linked Event"""
    root_tag = "essenceLinkedEvent"

    def __init__(self, xml):
        super().__init__(xml)
        self.file = self._get_xpath_from_event(XPATHS["file"])


class EssenceUnlinkedEvent(EssenceEvent):
    """Convenience class for an XML Essence Linked Event"""
    root_tag = "essenceUnlinkedEvent"


class ObjectDeletedEvent(EssenceEvent):
    """Convenience class for an XML Object Delete Event"""
    root_tag = "objectDeletedEvent"
