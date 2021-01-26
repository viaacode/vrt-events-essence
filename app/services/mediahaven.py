#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import functools
from typing import List, Tuple

import requests
import urllib
from lxml import etree
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException

from app.helpers.retry import retry, RetryException

NAMESPACE_MHS = "https://zeticon.mediahaven.com/metadata/20.1/mhs/"
NSMAP = {"mhs": NAMESPACE_MHS}


class AuthenticationException(Exception):
    """Exception raised when authentication fails."""
    pass


class MediahavenClient:
    def __init__(self, config: dict = None):
        self.cfg: dict = config
        self.token_info = None
        self.url = f'{self.cfg["mediahaven"]["host"]}/media/'

    def __authenticate(function):
        @functools.wraps(function)
        def wrapper_authenticate(self, *args, **kwargs):
            if not self.token_info:
                self.token_info = self.__get_token()
            try:
                return function(self, *args, **kwargs)
            except AuthenticationException:
                self.token_info = self.__get_token()
            return function(self, *args, **kwargs)

        return wrapper_authenticate

    def __get_token(self) -> str:
        """Gets an OAuth token that can be used in mediahaven requests to authenticate."""
        user: str = self.cfg["mediahaven"]["username"]
        password: str = self.cfg["mediahaven"]["password"]
        url: str = self.cfg["mediahaven"]["host"] + "/oauth/access_token"
        payload = {"grant_type": "password"}

        try:
            r = requests.post(
                url,
                auth=HTTPBasicAuth(user.encode("utf-8"), password.encode("utf-8")),
                data=payload,
            )

            if r.status_code != 201:
                raise RequestException(
                    f"Failed to get a token. Status: {r.status_code}"
                )
            token_info = r.json()
        except RequestException as e:
            raise e
        return token_info

    def _construct_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.token_info['access_token']}",
            "Accept": "application/vnd.mediahaven.v2+json",
        }

    @__authenticate
    def get_fragment(self, query_key_values: List[Tuple[str, object]]) -> dict:
        headers = self._construct_headers()

        # Construct URL query parameters as '+(k1:"v1") +(k2:"v2") +(k3:"v3") ...'
        query = " ".join([f'+({k_v[0]}: "{k_v[1]}")' for k_v in query_key_values])

        params_dict: dict = {
            "q": query,
        }
        # Encode the spaces in the query parameters as %20 and not +
        params = urllib.parse.urlencode(params_dict, quote_via=urllib.parse.quote)

        # Send the GET request
        response = requests.get(self.url, headers=headers, params=params,)

        if response.status_code == 401:
            # AuthenticationException triggers a retry with a new token
            raise AuthenticationException(response.text)

        # If there is an HTTP error, raise it
        response.raise_for_status()

        return response.json()

    @__authenticate
    def create_fragment(self, media_object_id: str) -> dict:
        headers = self._construct_headers()

        # Construct the URL to post to
        url = f'{self.url}{media_object_id}/fragments'

        # Create the payload
        data = {"fragmentStartFrames": "0", "fragmentEndFrames": "0"}

        # Send the POST request, as multipart/form-data
        response = requests.post(url, headers=headers, files=data)

        if response.status_code == 401:
            # AuthenticationException triggers a retry with a new token
            raise AuthenticationException(response.text)

        # If there is an HTTP error, raise it
        response.raise_for_status()

        return response.json()

    @__authenticate
    def delete_fragment(self, fragment_id: str, reason: str = "", event_type: str = "") -> dict:
        headers = self._construct_headers()

        # Construct the URL to post to
        url = f'{self.url}{fragment_id}'

        data = {"reason": reason, "eventType": event_type.upper()}

        # Send the DELETE request
        response = requests.delete(url, headers=headers, files=data)

        if response.status_code == 401:
            # AuthenticationException triggers a retry with a new token
            raise AuthenticationException(response.text)

        # If there is an HTTP error, raise it
        response.raise_for_status()

        return response.status_code == 204

    @retry(RetryException)
    @__authenticate
    def add_metadata_to_fragment(self, fragment_id: str, media_id: str, pid: str, ie_type: str) -> bool:
        headers = self._construct_headers()

        # Construct the URL to POST to
        url = f'{self.url}{fragment_id}'

        # Create the payload
        sidecar = self._construct_metadata(media_id, pid, ie_type)
        data = {"metadata": sidecar, "reason": "essenceLinked: add mediaID and PID to fragment"}

        # Send the POST request, as multipart/form-data
        response = requests.post(url, headers=headers, files=data)

        if response.status_code == 401:
            # AuthenticationException triggers a retry with a new token
            raise AuthenticationException(response.text)

        if response.status_code in (404, 403):
            raise RetryException(
                f"Unable to update metadata for fragment_id: {fragment_id} with status code: {response.status_code}",
            )

        # If there is an HTTP error, raise it
        response.raise_for_status()

        return response.status_code == 204

    def _construct_metadata(self, media_id: str, pid: str, ie_type: str) -> str:
        """Create the sidecar XML to upload the metadata.

        Returns:
            str -- The metadata sidecar XML.
        """
        root = etree.Element(f"{{{NAMESPACE_MHS}}}Sidecar", nsmap=NSMAP, version="20.1")
        # /Dynamic
        dynamic = etree.SubElement(root, f"{{{NAMESPACE_MHS}}}Dynamic")
        # /Dynamic/dc_identifier_localid
        etree.SubElement(dynamic, "dc_identifier_localid").text = media_id
        # /Dynamic/PID
        etree.SubElement(dynamic, "PID").text = pid
        # /Dynamic/dc_identifier_localids
        local_ids = etree.SubElement(dynamic, "dc_identifier_localids")
        # /Dynamic/dc_identifier_localids/MEDIA_ID
        etree.SubElement(local_ids, "MEDIA_ID").text = media_id
        # /Dynamic/object_level
        etree.SubElement(dynamic, "object_level").text = "ie"
        # /Dynamic/object_use
        etree.SubElement(dynamic, "object_use").text = "archive_master"
        # /Dynamic/ie_type
        etree.SubElement(dynamic, "ie_type").text = ie_type

        return etree.tostring(
            root,
            pretty_print=True,
            encoding="UTF-8",
            xml_declaration=True
        ).decode("utf-8")
