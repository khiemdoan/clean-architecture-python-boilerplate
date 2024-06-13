__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'
__source__ = 'https://github.com/khiemdoan/clean-architecture-python-boilerplate/blob/main/src/connectors/database.py'

from asyncio import current_task
from typing import Annotated, AsyncGenerator, Generator

from fast_depends import Depends
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_scoped_session,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from settings import PostgresSettings


class Database:
    _sync_engine: Engine = None
    _async_engine: AsyncEngine = None

    @classmethod
    def get_sync_session(cls) -> Generator[Session, None, None]:
        if cls._sync_engine is None:
            settings = PostgresSettings()
            cls._sync_engine = create_engine(settings.url, echo=settings.debug)

        try:
            session = sessionmaker(bind=cls._sync_engine)
            session = scoped_session(session)
            yield session()
        finally:
            session.remove()

    @classmethod
    async def get_async_session(cls) -> AsyncGenerator[AsyncSession, None]:
        if cls._async_engine is None:
            settings = PostgresSettings()
            cls._async_engine = create_async_engine(settings.url, echo=settings.debug)

        try:
            session = async_sessionmaker(bind=cls._async_engine)
            session = async_scoped_session(session, current_task)
            yield session()
        finally:
            await session.remove()


DbSyncSession = Annotated[Session, Depends(Database.get_sync_session)]
DbAsyncSession = Annotated[AsyncSession, Depends(Database.get_async_session)]
