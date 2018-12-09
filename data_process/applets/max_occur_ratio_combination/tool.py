"""
Common classes and functions.

Author: lj1218
Date  : 2018-12-09
"""
import time


class Timer(object):
    """Timer."""

    def __init__(self):
        """Init timer."""
        self.__start_time = None
        self.__start()

    def __start(self):
        """Start timer."""
        self.__start_time = time.time()

    def elapse(self, ndigits=None):
        """Get elapse time."""
        return round(time.time() - self.__start_time, ndigits)

    def reset(self):
        """Reset timer."""
        self.__start()
