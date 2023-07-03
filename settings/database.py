
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'

from functools import lru_cache
from urllib.parse import quote_plus

from pydantic import PostgresDsn, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    host: str
    port: str
    user: str
    password: str
    database: str

    model_config = SettingsConfigDict(extra='ignore', env_prefix='DATABASE_')

    @property
    def url(self) -> PostgresDsn:
        scheme = 'postgresql+psycopg'
        user = quote_plus(self.user)
        password = quote_plus(self.password)
        host = self.host
        port = self.port
        return f'{scheme}://{user}:{password}@{host}:{port}/{self.database}'


@lru_cache
def get_database_settings() -> DatabaseSettings:
    try:
        return DatabaseSettings()
    except ValidationError:
        pass

    return DatabaseSettings(_env_file='.env')
