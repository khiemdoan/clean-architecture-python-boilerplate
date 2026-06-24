__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'
__url__ = 'https://github.com/khiemdoan/clean-architecture-python-boilerplate/blob/main/src/settings/valkey.py'

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ValkeySettings(BaseSettings):
    host: str = 'localhost'
    port: int = Field(gt=0, le=65535, default=6379)
    password: str

    model_config = SettingsConfigDict(
        extra='ignore',
        env_prefix='VALKEY_',
        env_file='.env',
        env_file_encoding='utf-8',
    )
