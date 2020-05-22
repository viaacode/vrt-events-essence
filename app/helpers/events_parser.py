#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from io import BytesIO
from lxml import etree

# Constants
VRT_NAMESPACE = "http://www.vrt.be/mig/viaa/api"


class EssenceLinkedEventNotValidException(Exception):
    pass


class EssenceLinkedEvent:
    """Convenience class for an XML Essence Linked Event"""
    def __init__(self, xml):
        self.xml_element = self._get_essence_linked_event(xml)
        self.timestamp = self._get_timestamp()
        self.file = self._get_file()
        self.media_id = self._get_media_ID()

    def _get_essence_linked_event(self, xml: str):
        """Parse the input XML to a DOM"""
        tree = etree.parse(BytesIO(xml))
        return tree.xpath(
            "/p:essenceLinkedEvent", namespaces={"p": VRT_NAMESPACE}
        )[0]

    def _get_timestamp(self) -> str:
        return self.xml_element.xpath(
            "./p:timestamp", namespaces={"p": VRT_NAMESPACE}
        )[0].text

    def _get_file(self) -> str:
        return self.xml_element.xpath(
            "./p:file", namespaces={"p": VRT_NAMESPACE}
        )[0].text

    def _get_media_ID(self) -> str:
        return self.xml_element.xpath(
            "./p:mediaId", namespaces={"p": VRT_NAMESPACE}
        )[0].text
