#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from requests.exceptions import RequestException

from app.helpers.retry import retry
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

    @retry((RequestException, IndexError, KeyError))
    def get_pid(self) -> str:
        """ Fetches a PID via a GET call to the PID Service endpoint """
        resp = requests.get(self.url)
        log.debug(f"Response is: {resp.raw}")
        pid = resp.json()[0]["id"]

        return pid
