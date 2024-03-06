__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'
__url__ = 'https://github.com/khiemdoan/clean-architecture-python-boilerplate/blob/main/src/services/database.py'

import inspect
from asyncio import current_task
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from functools import wraps
from inspect import iscoroutinefunction
from typing import Callable, ParamSpec, TypeVar

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, async_scoped_session, async_sessionmaker, create_async_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from settings import get_postgres_settings

Param = ParamSpec('Param')
RetType = TypeVar('RetType')
OriginalFunc = Callable[Param, RetType]
DecoratedFunc = Callable[Param, RetType]


class Database(AbstractContextManager, AbstractAsyncContextManager):
    _SyncScopedSession: scoped_session[Session] | None = None
    _AsyncScopedSession: async_scoped_session[AsyncSession] | None = None

    @classmethod
    def connect(cls) -> None:
        if cls._SyncScopedSession and cls._AsyncScopedSession:
            return

        settings = get_postgres_settings()
        options = {
            'pool_size': 10,
            'max_overflow': 10,
        }

        sync_engine = create_engine(settings.url, echo=settings.debug, **options)
        sync_factory = sessionmaker(bind=sync_engine)
        cls._SyncScopedSession = scoped_session(sync_factory)

        async_engine = create_async_engine(settings.url, echo=settings.debug, **options)
        async_factory = async_sessionmaker(bind=async_engine)
        cls._AsyncScopedSession = async_scoped_session(async_factory, current_task)

    @classmethod
    def decorator(cls, func) -> Callable[[OriginalFunc], DecoratedFunc]:
        fsig = inspect.signature(func)

        if iscoroutinefunction(func):

            @wraps(func)
            async def wrapper(*args, **kwargs):
                cls.connect()
                session = cls._AsyncScopedSession()

                # Prepare params
                for _, param in fsig.parameters.items():
                    if param.annotation == type(session):
                        kwargs[param.name] = session

                # Call function
                try:
                    return await func(*args, **kwargs)
                finally:
                    await cls._AsyncScopedSession.remove()

            return wrapper
        else:

            @wraps(func)
            def wrapper(*args, **kwargs):
                cls.connect()
                session = cls._SyncScopedSession()

                # Prepare params
                for _, param in fsig.parameters.items():
                    if param.annotation == type(session):
                        kwargs[param.name] = session

                # Call function
                try:
                    return func(*args, **kwargs)
                finally:
                    cls._SyncScopedSession.remove()

            return wrapper

    def __enter__(self) -> Session:
        self.connect()
        return self._SyncScopedSession()

    def __exit__(self, *exc) -> None:
        self._SyncScopedSession.remove()

    async def __aenter__(self) -> AsyncSession:
        self.connect()
        return self._AsyncScopedSession()

    async def __aexit__(self, *exc) -> None:
        await self._AsyncScopedSession.remove()
