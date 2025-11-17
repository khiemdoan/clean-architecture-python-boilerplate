
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'

import sys
from pathlib import Path

from loguru import logger


def setup_logging(filename: str) -> None:
    logger.remove()

    # Write all logs to console
    time = '<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green>'
    level = '<level>{level: <8}</level>'
    locate = '<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan>'
    message = '<level>{message}</level>'
    if _check_inside_container():
        format = f'{level} | {locate} - {message}'
    else:
        format = f'{time} | {level} | {locate} - {message}'
    logger.add(sys.stdout, level='INFO', enqueue=True, format=format, colorize=True)

    default = {
        'enqueue': True,
        'rotation': '2MB',
        'retention': 5,
        'compression': 'tar.gz',
    }

    # Write only error logs to file
    format = '{time} | {level: <8} | {name}:{function}:{line} - {message}'
    logger.add(f'logs/{filename}-error.log', level='ERROR', format=format, **default)

    # Write all logs to file
    logger.add(f'logs/{filename}.log', level='INFO', serialize=True, **default)


def _check_inside_container() -> bool:
    # https://stackoverflow.com/questions/23513045/how-to-check-if-a-process-is-running-inside-docker-container
    if Path('/.dockerenv').is_file():           # Docker container
        return True
    if Path('/run/.containerenv').is_file():    # Podman container
        return True
    return False
