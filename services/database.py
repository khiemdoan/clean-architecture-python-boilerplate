
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'

import inspect
import logging
from contextlib import AbstractAsyncContextManager
from functools import wraps
from typing import Callable, ParamSpec, TypeVar

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from settings import get_postgres_settings

logger = logging.getLogger(__name__)

Param = ParamSpec('Param')
RetType = TypeVar('RetType')
OriginalFunc = Callable[Param, RetType]
DecoratedFunc = Callable[Param, RetType]


class Database(AbstractAsyncContextManager):

    _factory: async_sessionmaker|None = None

    @classmethod
    def connect(cls) -> None:
        if cls._factory:
            return
        settings = get_postgres_settings()
        engine = create_async_engine(settings.url, echo=settings.debug)
        cls._factory = async_sessionmaker(bind=engine)

    @classmethod
    def decorator(cls, func) -> Callable[[OriginalFunc], DecoratedFunc]:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            cls.connect()
            async with cls._factory() as session:
                fsig = inspect.signature(func)
                for _, param in fsig.parameters.items():
                    if param.annotation == AsyncSession:
                        kwargs[param.name] = session
                result = await func(*args, **kwargs)
                try:
                    await session.commit()
                except Exception as ex:
                    logger.error(ex)
                    await session.rollback()
                finally:
                    await session.close()
            return result

        return wrapper

    async def __aenter__(self) -> AsyncSession:
        self.connect()
        self._session = self._factory()
        return self._session

    async def __aexit__(self, *exc):
        try:
            await self._session.commit()
        except Exception as ex:
            logger.error(ex)
            await self._session.rollback()
        finally:
            await self._session.close()
