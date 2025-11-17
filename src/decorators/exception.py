__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'
__url__ = 'https://github.com/khiemdoan/clean-architecture-python-boilerplate/blob/main/src/decorators/exception.py'

from functools import wraps
from inspect import iscoroutinefunction
from typing import Protocol


class LoggerProtocol(Protocol):
    def exception(self, msg: str) -> None: ...


def ignore_exception(logger: LoggerProtocol | None = None):
    def inner(func):
        if iscoroutinefunction(func):

            @wraps(func)
            async def wrapper(*args, **kwargs):
                try:
                    return await func(*args, **kwargs)
                except Exception as ex:
                    if logger is not None:
                        logger.exception(ex)
        else:

            @wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    if logger is not None:
                        logger.exception(ex)

        return wrapper

    return inner
