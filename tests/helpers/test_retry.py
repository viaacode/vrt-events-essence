from unittest.mock import MagicMock, patch
from app.helpers.retry import retry, RetryException, NUMBER_OF_TRIES, DELAY, BACKOFF


@patch('time.sleep')
def test_retry(time_sleep_mock):
    function_mock = MagicMock()
    function_mock.side_effect = RetryException

    @retry(RetryException)
    def func(self):
        function_mock()

    # Execute the decorated method
    func(MagicMock())

    # Test if function was executed multiple times
    assert function_mock.call_count == NUMBER_OF_TRIES

    # Test if time.sleep was executed multiple times
    assert time_sleep_mock.call_count == NUMBER_OF_TRIES

    # Test exponential backoff
    assert time_sleep_mock.call_args_list[0][0][0] == DELAY
    for i in range(1, NUMBER_OF_TRIES):
        prev_val = time_sleep_mock.call_args_list[i-1][0][0]
        assert time_sleep_mock.call_args_list[i][0][0] == prev_val*BACKOFF
