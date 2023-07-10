
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'

from functools import lru_cache
from urllib.parse import quote_plus

from pydantic import AmqpDsn, Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class RabbitmqSettings(BaseSettings):
    host: str
    port: int = Field(gt=0, le=65535, default=5672)
    user: str
    password: str

    model_config = SettingsConfigDict(extra='ignore', env_prefix='RABBITMQ_')

    @property
    def url(self) -> AmqpDsn:
        scheme = 'amqp'
        user = quote_plus(self.user)
        password = quote_plus(self.password)
        host = self.host
        port = self.port
        return f'{scheme}://{user}:{password}@{host}:{port}/'


@lru_cache
def get_rabbitmq_settings() -> RabbitmqSettings:
    try:
        return RabbitmqSettings()
    except ValidationError:
        pass

    return RabbitmqSettings(_env_file='.env')
