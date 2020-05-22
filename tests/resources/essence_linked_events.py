#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os


essence_linked_event_fn = os.path.join(
    os.path.dirname(__file__), "essence_linked_event.xml"
)
with open(essence_linked_event_fn, "rb") as f:
    essence_linked_event = f.read()
