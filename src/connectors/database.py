__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'
__source__ = 'https://github.com/khiemdoan/clean-architecture-python-boilerplate/blob/main/src/connectors/database.py'

from asyncio import current_task
from typing import Annotated, AsyncGenerator, Generator
from urllib.parse import quote_plus

from fast_depends import Depends
from loguru import logger
from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import Engine, create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_scoped_session,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, scoped_session, sessionmaker


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
        return PostgresDsn(f'{scheme}://{user}:{password}@{host}:{port}/{self.database}')


class _Postgres:
    _sync_engine: Engine = None
    _async_engine: AsyncEngine = None

    @classmethod
    def get_sync_session(cls) -> Generator[Session, None, None]:
        if cls._sync_engine is None:
            settings = PostgresSettings()
            cls._sync_engine = create_engine(
                str(settings.url),
                pool_pre_ping=True,
                echo=settings.debug,
                connect_args={'application_name': settings.app_name},
            )

        try:
            maker = sessionmaker(bind=cls._sync_engine)
            scoped = scoped_session(maker)
            session = scoped()
            yield session
        finally:
            try:
                session.commit()
            except SQLAlchemyError as ex:
                logger.exception(ex)
                session.rollback()
            finally:
                scoped.remove()

    @classmethod
    async def get_async_session(cls) -> AsyncGenerator[AsyncSession, None]:
        if cls._async_engine is None:
            settings = PostgresSettings()
            cls._async_engine = create_async_engine(
                str(settings.url),
                pool_pre_ping=True,
                echo=settings.debug,
                connect_args={'application_name': settings.app_name},
            )

        try:
            maker = async_sessionmaker(bind=cls._async_engine)
            scoped = async_scoped_session(maker, current_task)
            session = scoped()
            yield session
        finally:
            try:
                await session.commit()
            except SQLAlchemyError as ex:
                logger.exception(ex)
                await session.rollback()
            finally:
                await scoped.remove()


DbSyncSession = Annotated[Session, Depends(_Postgres.get_sync_session)]
DbAsyncSession = Annotated[AsyncSession, Depends(_Postgres.get_async_session)]
