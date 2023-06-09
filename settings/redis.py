
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'

from functools import lru_cache

from pydantic import BaseSettings, Field, RedisDsn, validator
from pydantic.error_wrappers import ValidationError


class RedisSettings(BaseSettings):

    host: str = Field(env='REDIS_HOST')
    port: str = Field(env='REDIS_PORT')
    url: str = None

    @validator('url')
    def get_url(cls, _, values: dict[str, str]) -> str:
        return RedisDsn.build(
            scheme='redis',
            host=values.get('host'),
            port=values.get('port'),
        )


@lru_cache
def get_redis_settings() -> RedisSettings:
    try:
        return RedisSettings()
    except ValidationError:
        pass

    return RedisSettings(_env_file='.env')
