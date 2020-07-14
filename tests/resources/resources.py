#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os


def construct_filename(name):
    __location__ = os.path.realpath(
        os.path.join(os.getcwd(), os.path.dirname(__file__))
    )

    return os.path.join(__location__, f"{name}")


def load_resource(name):
    content = open(construct_filename(name), "rb").read()
    return content
