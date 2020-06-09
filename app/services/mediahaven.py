#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from lxml import etree

import functools

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException


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
    def get_fragment(self, query_key: str, value: str) -> dict:
        headers = self._construct_headers()

        # Construct URL query parameters
        params: dict = {
            "q": f'%2b({query_key}:"{value}")',
            "nrOfResults": 1,
        }

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
    def add_metadata_to_fragment(self, fragment_id: str, media_id: str) -> bool:
        headers = self._construct_headers()

        # Construct the URL to POST to
        url = f'{self.url}{fragment_id}'

        # Create the payload
        sidecar = self._construct_metadata(media_id)
        data = {"metadata": sidecar, "reason": "essenceLinked: add mediaID to fragment"}

        # Send the POST request, as multipart/form-data
        response = requests.post(url, headers=headers, files=data)

        if response.status_code == 401:
            # AuthenticationException triggers a retry with a new token
            raise AuthenticationException(response.text)

        # If there is an HTTP error, raise it
        response.raise_for_status()

        return True

    def _construct_metadata(self, media_id: str) -> str:
        """Create the sidecar XML to upload the media id metadata.

        TODO Rework this into the XML_BUILDER.

        Returns:
            str -- The metadata sidecar XML.
        """
        root = etree.Element(f"{{{NAMESPACE_MHS}}}Sidecar", nsmap=NSMAP, version="20.1")
        dynamic = etree.SubElement(root, f"{{{NAMESPACE_MHS}}}Dynamic")
        etree.SubElement(dynamic, "dc_identifier_localid").text = media_id
        local_ids = etree.SubElement(dynamic, "dc_identifier_localids")
        etree.SubElement(local_ids, "MEDIA_ID").text = media_id

        return etree.tostring(
            root,
            pretty_print=True,
            encoding="UTF-8",
            xml_declaration=True
        ).decode("utf-8")
