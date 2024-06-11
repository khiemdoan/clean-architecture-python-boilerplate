__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'
__url__ = 'https://github.com/khiemdoan/clean-architecture-python-boilerplate/blob/main/src/settings/rabbitmq.py'

from urllib.parse import quote_plus

from pydantic import AmqpDsn, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RabbitmqSettings(BaseSettings):
    host: str = 'localhost'
    port: int = Field(gt=0, le=65535, default=5672)
    user: str
    password: str
    tls: bool = False

    model_config = SettingsConfigDict(
        extra='ignore',
        env_prefix='RABBITMQ_',
        env_file='.env',
        env_file_encoding='utf-8',
    )

    @property
    def url(self) -> AmqpDsn:
        scheme = 'amqps' if self.tls else 'amqp'
        user = quote_plus(self.user)
        password = quote_plus(self.password)
        host = self.host
        port = self.port
        return f'{scheme}://{user}:{password}@{host}:{port}//'
