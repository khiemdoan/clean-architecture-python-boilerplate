__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'
__url__ = 'https://github.com/khiemdoan/clean-architecture-python-boilerplate/blob/main/src/settings/postgres.py'

from urllib.parse import quote_plus

from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseSettings):
    host: str = 'localhost'
    port: int = Field(gt=0, le=65535, default=5432)
    user: str
    password: str
    database: str
    app_name: str = ''
    debug: bool = False

    model_config = SettingsConfigDict(
        extra='ignore',
        env_prefix='POSTGRES_',
        env_file='.env',
        env_file_encoding='utf-8',
    )

    @property
    def url(self) -> PostgresDsn:
        scheme = 'postgresql+psycopg'
        user = quote_plus(self.user)
        password = quote_plus(self.password)
        host = self.host
        port = self.port
        return f'{scheme}://{user}:{password}@{host}:{port}/{self.database}'
