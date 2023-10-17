
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'
__url__ = 'https://github.com/khiemdoan/clean-architecture-python-boilerplate/blob/main/src/decorators/timeit.py'

import gc
from functools import wraps
from inspect import iscoroutinefunction
from logging import Logger
from time import perf_counter


def timeit(logger: Logger|None = None):
    def inner(func):
        if iscoroutinefunction(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                is_gc = gc.isenabled()
                if is_gc:
                    gc.disable()
                start_time = perf_counter()
                try:
                    return await func(*args, **kwargs)
                finally:
                    end_time = perf_counter()
                    if is_gc:
                        gc.enable()
                    duration = (end_time - start_time) * 1000
                    content_1 = f'Func: {func.__name__} with args: {args!r}, kwargs: {kwargs!r}'
                    content_2 = f'took: {duration:.3f} ms'
                    content = f'{content_1} {content_2}'
                    if isinstance(logger, Logger):
                        logger.warning(content)
                    else:
                        print(content)
        else:
            @wraps(func)
            def wrapper(*args, **kwargs):
                is_gc = gc.isenabled()
                if is_gc:
                    gc.disable()
                start_time = perf_counter()
                try:
                    return func(*args, **kwargs)
                finally:
                    end_time = perf_counter()
                    if is_gc:
                        gc.enable()
                    duration = (end_time - start_time) * 1000
                    content_1 = f'Func: {func.__name__} with args: {args!r}, kwargs: {kwargs!r}'
                    content_2 = f'took: {duration:.3f} ms'
                    content = f'{content_1} {content_2}'
                    if isinstance(logger, Logger):
                        logger.warning(content)
                    else:
                        print(content)
        return wrapper
    return inner
