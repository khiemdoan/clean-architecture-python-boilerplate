
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'
__url__ = 'https://github.com/khiemdoan/clean-architecture-python-boilerplate/blob/main/src/settings/__init__.py'

__all__ = [
    'MariadbSettings', 'get_mariadb_settings',
    'PostgresSettings', 'get_postgres_settings',
    'RabbitmqSettings', 'get_rabbitmq_settings',
    'RedisSettings', 'get_redis_settings',
    'TelegramSettings', 'get_telegram_settings',
]


from .mariadb import MariadbSettings, get_mariadb_settings
from .postgres import PostgresSettings, get_postgres_settings
from .rabbitmq import RabbitmqSettings, get_rabbitmq_settings
from .redis import RedisSettings, get_redis_settings
from .telegram import TelegramSettings, get_telegram_settings
