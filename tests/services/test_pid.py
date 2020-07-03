#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest.mock import patch

import pytest
from requests.exceptions import RequestException

from app.helpers.retry import NUMBER_OF_TRIES
from app.services.pid import PIDService

PID = "j96059k22s"
VALID_DICT = {
    "id": f"{PID}",
    "number": 1,
}
VALID_RESPONSE = [VALID_DICT]


class TestPIDservice:
    @pytest.fixture
    def pid_service(self):
        return PIDService("http://localhost")

    @patch('requests.get')
    def test_get_pid(self, get_mock, pid_service):
        get_mock().json.return_value = VALID_RESPONSE
        assert pid_service.get_pid() == PID

    @patch('time.sleep')
    @patch('requests.get', side_effect=RequestException)
    def test_get_pid_request_exception(self, get_mock, time_sleep_mock, pid_service):
        assert not pid_service.get_pid()
        assert time_sleep_mock.call_count == NUMBER_OF_TRIES

    @pytest.mark.parametrize("error", [IndexError, KeyError])
    @patch('time.sleep')
    @patch('requests.get')
    def test_get_pid_parse_error(self, get_mock, time_sleep_mock, error, pid_service):
        get_mock().json.side_effect = IndexError
        assert not pid_service.get_pid()
        assert time_sleep_mock.call_count == NUMBER_OF_TRIES
