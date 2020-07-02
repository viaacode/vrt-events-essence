#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from requests.exceptions import RequestException

from viaa.configuration import ConfigParser
from viaa.observability import logging

configParser = ConfigParser()
log = logging.get_logger(__name__, config=configParser)


class PIDService():
    """Abstraction to the pid-generating service.
    See: https://github.com/viaacode/pid_webservice
    The service returns a JSON as such:
    ```json
    [
        {
            "id": "j96059k22s",
            "number": 1
        }
    ]
    ```
    """

    def __init__(self, url: str):
        self.url = url

    def get_pid(self) -> str:
        """ Fetches a PID via a GET call to the PID Service endpoint """
        try:
            resp = requests.get(self.url)
        except RequestException:
            return

        log.debug(f"Response is: {resp.raw}")

        try:
            pid = resp.json()[0]["id"]
        except (IndexError, KeyError):
            return

        return pid
