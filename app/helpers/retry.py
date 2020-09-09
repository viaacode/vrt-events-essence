import functools
import time

from viaa.observability import logging

log = logging.get_logger(__name__)
log.setLevel("DEBUG")


class RetryException(Exception):
    """ Exception raised when an action needs to be retried
    in combination with _retry decorator"""

    pass


DELAY = 1
BACKOFF = 2
NUMBER_OF_TRIES = 5


def retry(exceptions):
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            delay = DELAY
            tries = NUMBER_OF_TRIES
            while tries:
                tries -= 1
                try:
                    return func(self, *args, **kwargs)
                except exceptions as error:
                    log.debug(
                        f"{error}. Retrying in {delay} seconds.",
                        try_count=NUMBER_OF_TRIES - tries,
                    )
                    time.sleep(delay)
                    delay *= BACKOFF
            return False

        return wrapper

    return decorator_retry
