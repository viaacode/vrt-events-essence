#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from lxml import etree


class XMLBuilder(object):
    XML_ENCODING = "UTF-8"
    NAMESPACES = {
        None: "http://www.vrt.be/mig/viaa/api",
        "ebu": "urn:ebu:metadata-schema:ebuCore_2012",
    }

    def __init__(self, ctx=None):
        self.xml = None
        self.ctx = ctx

    def build(self, root, metadata_dict) -> None:
        root = etree.Element(root, nsmap=self.NAMESPACES)
        # Add the subelements
        for sub, val in metadata_dict.items():
            etree.SubElement(root, sub).text = val
        self.xml = root

    def to_bytes(self, pretty=False) -> bytes:
        return etree.tostring(
            self.xml,
            pretty_print=pretty,
            encoding=self.XML_ENCODING,
            xml_declaration=True,
        )

    def to_string(self, pretty=False) -> str:
        return etree.tostring(
            self.xml,
            pretty_print=pretty,
            encoding=self.XML_ENCODING,
            xml_declaration=True,
        ).decode("utf-8")
