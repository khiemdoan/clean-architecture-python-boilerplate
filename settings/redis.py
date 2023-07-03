
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'

from functools import lru_cache
from urllib.parse import quote_plus

from pydantic import RedisDsn, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisSettings(BaseSettings):
    host: str
    port: str
    password: str

    model_config = SettingsConfigDict(extra='ignore', env_prefix='REDIS_')

    @property
    def url(self) -> RedisDsn:
        scheme = 'redis'
        password = quote_plus(self.password)
        host = self.host
        port = self.port
        return f'{scheme}://:{password}@{host}:{port}/0'


@lru_cache
def get_redis_settings() -> RedisSettings:
    try:
        return RedisSettings()
    except ValidationError:
        pass

    return RedisSettings(_env_file='.env')
