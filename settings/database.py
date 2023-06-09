
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'

from functools import lru_cache
from urllib.parse import quote_plus

from pydantic import BaseSettings, Field, PostgresDsn, validator
from pydantic.error_wrappers import ValidationError


class DatabaseSettings(BaseSettings):

    host: str = Field(env='DATABASE_HOST')
    port: str = Field(env='DATABASE_PORT')
    user: str = Field(env='DATABASE_USER')
    password: str = Field(env='DATABASE_PASSWORD')
    database: str = Field(env='DATABASE_DB')
    url: str = None

    @validator('url')
    def get_url(cls, _, values: dict[str, str]) -> str:
        return PostgresDsn.build(
            scheme='postgresql+psycopg',
            host=values.get('host'),
            port=values.get('port'),
            user=quote_plus(values.get('user')),
            password=quote_plus(values.get('password')),
            path=f"/{values.get('database')}",
        )


@lru_cache
def get_database_settings() -> DatabaseSettings:
    try:
        return DatabaseSettings()
    except ValidationError:
        pass

    return DatabaseSettings(_env_file='.env')
