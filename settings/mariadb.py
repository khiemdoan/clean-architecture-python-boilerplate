
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'

from functools import lru_cache
from urllib.parse import quote_plus

from pydantic import Field, MariaDBDsn, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class MariadbSettings(BaseSettings):
    host: str
    port: int = Field(gt=0, le=65535, default=3306)
    user: str
    password: str
    database: str
    debug: bool = False

    model_config = SettingsConfigDict(extra='ignore', env_prefix='MARIADB_')

    @property
    def url(self) -> MariaDBDsn:
        scheme = 'mariadb'
        user = quote_plus(self.user)
        password = quote_plus(self.password)
        host = self.host
        port = self.port
        return f'{scheme}://{user}:{password}@{host}:{port}/{self.database}'


@lru_cache
def get_mariadb_settings() -> MariadbSettings:
    try:
        return MariadbSettings()
    except ValidationError:
        pass

    return MariadbSettings(_env_file='.env')
