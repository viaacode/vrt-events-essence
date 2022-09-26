#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pytest

from app.helpers.events_parser import (
    EssenceLinkedEvent,
    EssenceUnlinkedEvent,
    ObjectDeletedEvent,
    InvalidEventException,
)
from tests.resources.resources import load_resource


INVALID_ESSENCE_LINKED_EVENTS = [
    "essenceLinkedEventFileMissing.xml",
    "essenceLinkedEventMediaIdMissing.xml",
    "essencelinkedEventTimestampMissing.xml",
]


INVALID_ESSENCE_UNLINKED_EVENTS = [
    "essenceUnlinkedEventMediaIdMissing.xml",
    "essenceUnlinkedEventTimestampMissing.xml",
]


INVALID_OBJECT_DELETED_EVENTS = [
    "objectDeletedEventMediaIdMissing.xml",
    "objectDeletedEventTimestampMissing.xml",
]


def test_essence_linked_event_valid():
    event = EssenceLinkedEvent(load_resource("essenceLinkedEvent.xml"))
    assert event.timestamp == "2019-09-24T17:21:28.787+02:00"
    assert event.file == "file.mxf"
    assert event.media_id == "media id"
    assert event.s3_bucket == "bucket"


@pytest.mark.parametrize("filename", INVALID_ESSENCE_LINKED_EVENTS)
def test_essence_linked_event_invalid(filename):
    with pytest.raises(InvalidEventException):
        EssenceLinkedEvent(load_resource(filename))


def test_essence_unlinked_event_valid():
    event = EssenceUnlinkedEvent(load_resource("essenceUnlinkedEvent.xml"))
    assert event.timestamp == "2019-09-24T17:21:28.787+02:00"
    assert event.media_id == "media id"


@pytest.mark.parametrize("filename", INVALID_ESSENCE_UNLINKED_EVENTS)
def test_essence_unlinked_event_invalid(filename):
    with pytest.raises(InvalidEventException):
        EssenceUnlinkedEvent(load_resource(filename))


def test_object_deleted_event_valid():
    event = ObjectDeletedEvent(load_resource("objectDeletedEvent.xml"))
    assert event.timestamp == "2019-09-24T17:21:28.787+02:00"
    assert event.media_id == "media id"


@pytest.mark.parametrize("filename", INVALID_OBJECT_DELETED_EVENTS)
def test_object_deleted_event_invalid(filename):
    with pytest.raises(InvalidEventException):
        ObjectDeletedEvent(load_resource(filename))


def test_invalid_xml():
    with pytest.raises(InvalidEventException) as error:
        EssenceLinkedEvent(b"")
    assert error.value.message == "Event is not valid XML."


def test_essence_linked_wrong_type():
    with pytest.raises(InvalidEventException) as error:
        EssenceLinkedEvent(load_resource("essenceUnlinkedEvent.xml"))
    assert EssenceLinkedEvent.root_tag in error.value.message
