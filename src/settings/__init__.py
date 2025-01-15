__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'
__url__ = 'https://github.com/khiemdoan/clean-architecture-python-boilerplate/blob/main/src/settings/__init__.py'

__all__ = [
    'MariadbSettings',
    'RabbitmqSettings',
    'RedisSettings',
    'TelegramSettings',
]


from .mariadb import MariadbSettings
from .rabbitmq import RabbitmqSettings
from .redis import RedisSettings
from .telegram import TelegramSettings
