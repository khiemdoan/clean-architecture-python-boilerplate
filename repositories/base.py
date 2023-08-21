
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'

# Original source from `litestar` project
# https://github.com/litestar-org/litestar/blob/main/litestar/contrib/repository/exceptions.py
# https://github.com/litestar-org/litestar/blob/main/litestar/contrib/repository/filters.py
# https://github.com/litestar-org/litestar/blob/main/litestar/contrib/repository/abc/_async.py
# https://github.com/litestar-org/litestar/blob/main/litestar/contrib/repository/abc/_sync.py
# https://github.com/litestar-org/litestar/blob/main/litestar/contrib/sqlalchemy/repository/_util.py
# https://github.com/litestar-org/litestar/blob/main/litestar/contrib/sqlalchemy/repository/_async.py
# https://github.com/litestar-org/litestar/blob/main/litestar/contrib/sqlalchemy/repository/_sync.py
# Modified by Khiem Doan

"""SQLAlchemy-based implementation of the repository protocol."""
from abc import ABCMeta, abstractmethod
from collections import abc
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generic, Iterable, Literal, Tuple, TypeAlias, TypeVar, cast

from sqlalchemy import Select, TextClause, delete
from sqlalchemy import func as sql_func
from sqlalchemy import over, select, text, update
from sqlalchemy.engine import Result
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute, Session

from models.base import ModelProtocol

T = TypeVar("T")
CollectionT = TypeVar("CollectionT")

ModelT = TypeVar("ModelT", bound=ModelProtocol)
SelectT = TypeVar("SelectT", bound=Select[Any])
RowT = TypeVar("RowT", bound=Tuple[Any, ...])


class RepositoryError(Exception):
    """Base repository exception type."""


class ConflictError(RepositoryError):
    """Data integrity error."""


class NotFoundError(RepositoryError):
    """An identity does not exist."""


@dataclass
class BeforeAfter:
    """Data required to filter a query on a ``datetime`` column."""

    field_name: str
    """Name of the model attribute to filter on."""
    before: datetime | None
    """Filter results where field earlier than this."""
    after: datetime | None
    """Filter results where field later than this."""


@dataclass
class OnBeforeAfter:
    """Data required to filter a query on a ``datetime`` column."""

    field_name: str
    """Name of the model attribute to filter on."""
    on_or_before: datetime | None
    """Filter results where field is on or earlier than this."""
    on_or_after: datetime | None
    """Filter results where field on or later than this."""


@dataclass
class CollectionFilter(Generic[T]):
    """Data required to construct a ``WHERE ... IN (...)`` clause."""

    field_name: str
    """Name of the model attribute to filter on."""
    values: abc.Collection[T]
    """Values for ``IN`` clause."""


@dataclass
class NotInCollectionFilter(Generic[T]):
    """Data required to construct a ``WHERE ... NOT IN (...)`` clause."""

    field_name: str
    """Name of the model attribute to filter on."""
    values: abc.Collection[T]
    """Values for ``NOT IN`` clause."""


@dataclass
class LimitOffset:
    """Data required to add limit/offset filtering to a query."""

    limit: int
    """Value for ``LIMIT`` clause of query."""
    offset: int
    """Value for ``OFFSET`` clause of query."""


@dataclass
class OrderBy:
    """Data required to construct a ``ORDER BY ...`` clause."""

    field_name: str
    """Name of the model attribute to sort on."""
    sort_order: Literal["asc", "desc"] = "asc"
    """Sort ascending or descending"""


@dataclass
class SearchFilter:
    """Data required to construct a ``WHERE field_name LIKE '%' || :value || '%'`` clause."""

    field_name: str
    """Name of the model attribute to sort on."""
    value: str
    """Values for ``LIKE`` clause."""
    ignore_case: bool | None = False
    """Should the search be case insensitive."""


@dataclass
class NotInSearchFilter:
    """Data required to construct a ``WHERE field_name NOT LIKE '%' || :value || '%'`` clause."""

    field_name: str
    """Name of the model attribute to search on."""
    value: str
    """Values for ``NOT LIKE`` clause."""
    ignore_case: bool | None = False
    """Should the search be case insensitive."""


FilterTypes: TypeAlias = BeforeAfter | OnBeforeAfter | CollectionFilter[Any] | LimitOffset | OrderBy | SearchFilter | NotInCollectionFilter[Any] | NotInSearchFilter
"""Aggregate type alias of the types supported for collection filtering."""


class AbstractAsyncRepository(Generic[T], metaclass=ABCMeta):
    """Interface for persistent data interaction."""

    model_type: type[T]
    """Type of object represented by the repository."""
    id_attribute: Any = "id"
    """Name of the primary identifying attribute on :attr:`model_type`."""

    def __init__(self, **kwargs: Any) -> None:
        """Repository constructors accept arbitrary kwargs."""
        super().__init__(**kwargs)

    @abstractmethod
    async def add(self, data: T) -> T:
        """Add ``data`` to the collection.

        Args:
            data: Instance to be added to the collection.

        Returns:
            The added instance.
        """

    @abstractmethod
    async def add_many(self, data: list[T]) -> list[T]:
        """Add multiple ``data`` to the collection.

        Args:
            data: Instances to be added to the collection.

        Returns:
            The added instances.
        """

    @abstractmethod
    async def count(self, *filters: FilterTypes, **kwargs: Any) -> int:
        """Get the count of records returned by a query.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            The count of instances
        """

    @abstractmethod
    async def delete(self, item_id: Any) -> T:
        """Delete instance identified by ``item_id``.

        Args:
            item_id: Identifier of instance to be deleted.

        Returns:
            The deleted instance.

        Raises:
            NotFoundError: If no instance found identified by ``item_id``.
        """

    @abstractmethod
    async def delete_many(self, item_ids: list[Any]) -> list[T]:
        """Delete multiple instances identified by list of IDs ``item_ids``.

        Args:
            item_ids: list of Identifiers to be deleted.

        Returns:
            The deleted instances.
        """

    @abstractmethod
    async def exists(self, **kwargs: Any) -> bool:
        """Return true if the object specified by ``kwargs`` exists.

        Args:
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            True if the instance was found.  False if not found.

        """

    @abstractmethod
    async def get(self, item_id: Any, **kwargs: Any) -> T:
        """Get instance identified by ``item_id``.

        Args:
            item_id: Identifier of the instance to be retrieved.
            **kwargs: Additional arguments

        Returns:
            The retrieved instance.

        Raises:
            NotFoundError: If no instance found identified by ``item_id``.
        """

    @abstractmethod
    async def get_one(self, **kwargs: Any) -> T:
        """Get an instance specified by the ``kwargs`` filters if it exists.

        Args:
            **kwargs: Instance attribute value filters.

        Returns:
            The retrieved instance.

        Raises:
            NotFoundError: If no instance found identified by ``kwargs``.
        """

    @abstractmethod
    async def get_or_create(self, **kwargs: Any) -> tuple[T, bool]:
        """Get an instance specified by the ``kwargs`` filters if it exists or create it.

        Args:
            **kwargs: Instance attribute value filters.

        Returns:
            A tuple that includes the retrieved or created instance, and a boolean on whether the record was created or not
        """

    @abstractmethod
    async def get_one_or_none(self, **kwargs: Any) -> T | None:
        """Get an instance if it exists or None.

        Args:
            **kwargs: Instance attribute value filters.

        Returns:
            The retrieved instance or None.
        """

    @abstractmethod
    async def update(self, data: T) -> T:
        """Update instance with the attribute values present on ``data``.

        Args:
            data: An instance that should have a value for :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` that exists in the
                collection.

        Returns:
            The updated instance.

        Raises:
            NotFoundError: If no instance found with same identifier as ``data``.
        """

    @abstractmethod
    async def update_many(self, data: list[T]) -> list[T]:
        """Update multiple instances with the attribute values present on instances in ``data``.

        Args:
            data: A list of instance that should have a value for :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` that exists in the
                collection.

        Returns:
            a list of the updated instances.

        Raises:
            NotFoundError: If no instance found with same identifier as ``data``.
        """

    @abstractmethod
    async def upsert(self, data: T) -> T:
        """Update or create instance.

        Updates instance with the attribute values present on ``data``, or creates a new instance if
        one doesn't exist.

        Args:
            data: Instance to update existing, or be created. Identifier used to determine if an
                existing instance exists is the value of an attribute on ``data`` named as value of
                :attr:`id_attribute <AbstractAsyncRepository.id_attribute>`.

        Returns:
            The updated or created instance.

        Raises:
            NotFoundError: If no instance found with same identifier as ``data``.
        """

    @abstractmethod
    async def upsert_many(self, data: list[T]) -> list[T]:
        """Update or create multiple instances.

        Update instances with the attribute values present on ``data``, or create a new instance if
        one doesn't exist.

        Args:
            data: Instances to update or created. Identifier used to determine if an
                existing instance exists is the value of an attribute on ``data`` named as value of
                :attr:`id_attribute <AbstractAsyncRepository.id_attribute>`.

        Returns:
            The updated or created instances.

        Raises:
            NotFoundError: If no instance found with same identifier as ``data``.
        """

    @abstractmethod
    async def list_and_count(self, *filters: FilterTypes, **kwargs: Any) -> tuple[list[T], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            a tuple containing The list of instances, after filtering applied, and a count of records returned by query, ignoring pagination.
        """

    @abstractmethod
    async def list(self, *filters: FilterTypes, **kwargs: Any) -> list[T]:
        """Get a list of instances, optionally filtered.

        Args:
            *filters: filters for specific filtering operations
            **kwargs: Instance attribute value filters.

        Returns:
            The list of instances, after filtering applied
        """

    @abstractmethod
    def filter_collection_by_kwargs(self, collection: CollectionT, /, **kwargs: Any) -> CollectionT:
        """Filter the collection by kwargs.

        Has ``AND`` semantics where multiple kwargs name/value pairs are provided.

        Args:
            collection: the objects to be filtered
            **kwargs: key/value pairs such that objects remaining in the collection after filtering
                have the property that their attribute named ``key`` has value equal to ``value``.


        Returns:
            The filtered objects

        Raises:
            RepositoryError: if a named attribute doesn't exist on :attr:`model_type <AbstractAsyncRepository.model_type>`.
        """

    @staticmethod
    def check_not_found(item_or_none: T | None) -> T:
        """Raise :class:`NotFoundError` if ``item_or_none`` is ``None``.

        Args:
            item_or_none: Item (:class:`T <T>`) to be tested for existence.

        Returns:
            The item, if it exists.
        """
        if item_or_none is None:
            raise NotFoundError("No item found when one was expected")
        return item_or_none

    @classmethod
    def get_id_attribute_value(cls, item: T | type[T], id_attribute: str | None = None) -> Any:
        """Get value of attribute named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` on ``item``.

        Args:
            item: Anything that should have an attribute named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` value.
            id_attribute: Allows customization of the unique identifier to use for model fetching.
                Defaults to `None`, but can reference any surrogate or candidate key for the table.

        Returns:
            The value of attribute on ``item`` named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>`.
        """
        return getattr(item, id_attribute if id_attribute is not None else cls.id_attribute)

    @classmethod
    def set_id_attribute_value(cls, item_id: Any, item: T, id_attribute: str | None = None) -> T:
        """Return the ``item`` after the ID is set to the appropriate attribute.

        Args:
            item_id: Value of ID to be set on instance
            item: Anything that should have an attribute named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` value.
            id_attribute: Allows customization of the unique identifier to use for model fetching.
                Defaults to `None`, but can reference any surrogate or candidate key for the table.

        Returns:
            Item with ``item_id`` set to :attr:`id_attribute <AbstractAsyncRepository.id_attribute>`
        """
        setattr(item, id_attribute if id_attribute is not None else cls.id_attribute, item_id)
        return item


class AbstractSyncRepository(Generic[T], metaclass=ABCMeta):
    """Interface for persistent data interaction."""

    model_type: type[T]
    """Type of object represented by the repository."""
    id_attribute: Any = "id"
    """Name of the primary identifying attribute on :attr:`model_type`."""

    def __init__(self, **kwargs: Any) -> None:
        """Repository constructors accept arbitrary kwargs."""
        super().__init__(**kwargs)

    @abstractmethod
    def add(self, data: T) -> T:
        """Add ``data`` to the collection.

        Args:
            data: Instance to be added to the collection.

        Returns:
            The added instance.
        """

    @abstractmethod
    def add_many(self, data: list[T]) -> list[T]:
        """Add multiple ``data`` to the collection.

        Args:
            data: Instances to be added to the collection.

        Returns:
            The added instances.
        """

    @abstractmethod
    def count(self, *filters: FilterTypes, **kwargs: Any) -> int:
        """Get the count of records returned by a query.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            The count of instances
        """

    @abstractmethod
    def delete(self, item_id: Any) -> T:
        """Delete instance identified by ``item_id``.

        Args:
            item_id: Identifier of instance to be deleted.

        Returns:
            The deleted instance.

        Raises:
            NotFoundError: If no instance found identified by ``item_id``.
        """

    @abstractmethod
    def delete_many(self, item_ids: list[Any]) -> list[T]:
        """Delete multiple instances identified by list of IDs ``item_ids``.

        Args:
            item_ids: list of Identifiers to be deleted.

        Returns:
            The deleted instances.
        """

    @abstractmethod
    def exists(self, **kwargs: Any) -> bool:
        """Return true if the object specified by ``kwargs`` exists.

        Args:
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            True if the instance was found.  False if not found.

        """

    @abstractmethod
    def get(self, item_id: Any, **kwargs: Any) -> T:
        """Get instance identified by ``item_id``.

        Args:
            item_id: Identifier of the instance to be retrieved.
            **kwargs: Additional arguments

        Returns:
            The retrieved instance.

        Raises:
            NotFoundError: If no instance found identified by ``item_id``.
        """

    @abstractmethod
    def get_one(self, **kwargs: Any) -> T:
        """Get an instance specified by the ``kwargs`` filters if it exists.

        Args:
            **kwargs: Instance attribute value filters.

        Returns:
            The retrieved instance.

        Raises:
            NotFoundError: If no instance found identified by ``kwargs``.
        """

    @abstractmethod
    def get_or_create(self, **kwargs: Any) -> tuple[T, bool]:
        """Get an instance specified by the ``kwargs`` filters if it exists or create it.

        Args:
            **kwargs: Instance attribute value filters.

        Returns:
            A tuple that includes the retrieved or created instance, and a boolean on whether the record was created or not
        """

    @abstractmethod
    def get_one_or_none(self, **kwargs: Any) -> T | None:
        """Get an instance if it exists or None.

        Args:
            **kwargs: Instance attribute value filters.

        Returns:
            The retrieved instance or None.
        """

    @abstractmethod
    def update(self, data: T) -> T:
        """Update instance with the attribute values present on ``data``.

        Args:
            data: An instance that should have a value for :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` that exists in the
                collection.

        Returns:
            The updated instance.

        Raises:
            NotFoundError: If no instance found with same identifier as ``data``.
        """

    @abstractmethod
    def update_many(self, data: list[T]) -> list[T]:
        """Update multiple instances with the attribute values present on instances in ``data``.

        Args:
            data: A list of instance that should have a value for :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` that exists in the
                collection.

        Returns:
            a list of the updated instances.

        Raises:
            NotFoundError: If no instance found with same identifier as ``data``.
        """

    @abstractmethod
    def upsert(self, data: T) -> T:
        """Update or create instance.

        Updates instance with the attribute values present on ``data``, or creates a new instance if
        one doesn't exist.

        Args:
            data: Instance to update existing, or be created. Identifier used to determine if an
                existing instance exists is the value of an attribute on ``data`` named as value of
                :attr:`id_attribute <AbstractAsyncRepository.id_attribute>`.

        Returns:
            The updated or created instance.

        Raises:
            NotFoundError: If no instance found with same identifier as ``data``.
        """

    @abstractmethod
    def upsert_many(self, data: list[T]) -> list[T]:
        """Update or create multiple instances.

        Update instances with the attribute values present on ``data``, or create a new instance if
        one doesn't exist.

        Args:
            data: Instances to update or created. Identifier used to determine if an
                existing instance exists is the value of an attribute on ``data`` named as value of
                :attr:`id_attribute <AbstractAsyncRepository.id_attribute>`.

        Returns:
            The updated or created instances.

        Raises:
            NotFoundError: If no instance found with same identifier as ``data``.
        """

    @abstractmethod
    def list_and_count(self, *filters: FilterTypes, **kwargs: Any) -> tuple[list[T], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            a tuple containing The list of instances, after filtering applied, and a count of records returned by query, ignoring pagination.
        """

    @abstractmethod
    def list(self, *filters: FilterTypes, **kwargs: Any) -> list[T]:
        """Get a list of instances, optionally filtered.

        Args:
            *filters: filters for specific filtering operations
            **kwargs: Instance attribute value filters.

        Returns:
            The list of instances, after filtering applied
        """

    @abstractmethod
    def filter_collection_by_kwargs(self, collection: CollectionT, /, **kwargs: Any) -> CollectionT:
        """Filter the collection by kwargs.

        Has ``AND`` semantics where multiple kwargs name/value pairs are provided.

        Args:
            collection: the objects to be filtered
            **kwargs: key/value pairs such that objects remaining in the collection after filtering
                have the property that their attribute named ``key`` has value equal to ``value``.


        Returns:
            The filtered objects

        Raises:
            RepositoryError: if a named attribute doesn't exist on :attr:`model_type <AbstractAsyncRepository.model_type>`.
        """

    @staticmethod
    def check_not_found(item_or_none: T | None) -> T:
        """Raise :class:`NotFoundError` if ``item_or_none`` is ``None``.

        Args:
            item_or_none: Item (:class:`T <T>`) to be tested for existence.

        Returns:
            The item, if it exists.
        """
        if item_or_none is None:
            raise NotFoundError("No item found when one was expected")
        return item_or_none

    @classmethod
    def get_id_attribute_value(cls, item: T | type[T], id_attribute: str | None = None) -> Any:
        """Get value of attribute named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` on ``item``.

        Args:
            item: Anything that should have an attribute named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` value.
            id_attribute: Allows customization of the unique identifier to use for model fetching.
                Defaults to `None`, but can reference any surrogate or candidate key for the table.

        Returns:
            The value of attribute on ``item`` named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>`.
        """
        return getattr(item, id_attribute if id_attribute is not None else cls.id_attribute)

    @classmethod
    def set_id_attribute_value(cls, item_id: Any, item: T, id_attribute: str | None = None) -> T:
        """Return the ``item`` after the ID is set to the appropriate attribute.

        Args:
            item_id: Value of ID to be set on instance
            item: Anything that should have an attribute named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` value.
            id_attribute: Allows customization of the unique identifier to use for model fetching.
                Defaults to `None`, but can reference any surrogate or candidate key for the table.

        Returns:
            Item with ``item_id`` set to :attr:`id_attribute <AbstractAsyncRepository.id_attribute>`
        """
        setattr(item, id_attribute if id_attribute is not None else cls.id_attribute, item_id)
        return item


@contextmanager
def wrap_sqlalchemy_exception() -> Any:
    """Do something within context to raise a `RepositoryError` chained
    from an original `SQLAlchemyError`.

        >>> try:
        ...     with wrap_sqlalchemy_exception():
        ...         raise SQLAlchemyError("Original Exception")
        ... except RepositoryError as exc:
        ...     print(f"caught repository exception from {type(exc.__context__)}")
        ...
        caught repository exception from <class 'sqlalchemy.exc.SQLAlchemyError'>
    """
    try:
        yield
    except IntegrityError as exc:
        raise ConflictError from exc
    except SQLAlchemyError as exc:
        raise RepositoryError(f"An exception occurred: {exc}") from exc
    except AttributeError as exc:
        raise RepositoryError from exc


def get_instrumented_attr(model: type[ModelProtocol], key: str | InstrumentedAttribute) -> InstrumentedAttribute:
    if isinstance(key, str):
        return cast("InstrumentedAttribute", getattr(model, key))
    return key


class SQLAlchemyAsyncRepository(AbstractAsyncRepository[ModelT], Generic[ModelT]):
    """SQLAlchemy based implementation of the repository interface."""

    match_fields: list[str] | str | None = None
    id_attribute: str | InstrumentedAttribute

    def __init__(
        self,
        *,
        statement: Select[tuple[ModelT]] | None = None,
        session: AsyncSession,
        auto_expunge: bool = False,
        auto_refresh: bool = True,
        auto_commit: bool = False,
        **kwargs: Any,
    ) -> None:
        """Repository pattern for SQLAlchemy models.

        Args:
            statement: To facilitate customization of the underlying select query.
            session: Session managing the unit-of-work for the operation.
            auto_expunge: Remove object from session before returning.
            auto_refresh: Refresh object from session before returning.
            auto_commit: Commit objects before returning.
            **kwargs: Additional arguments.

        """
        super().__init__(**kwargs)
        self.auto_expunge = auto_expunge
        self.auto_refresh = auto_refresh
        self.auto_commit = auto_commit
        self.session = session
        self.statement = statement if statement is not None else select(self.model_type)
        if not self.session.bind:
            # this shouldn't actually ever happen, but we include it anyway to properly
            # narrow down the types
            raise ValueError("Session improperly configure")
        self._dialect = self.session.bind.dialect

    async def add(
        self,
        data: ModelT,
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
        auto_refresh: bool | None = None,
    ) -> ModelT:
        """Add `data` to the collection.

        Args:
            data: Instance to be added to the collection.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_refresh: Refresh object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_refresh <SQLAlchemyAsyncRepository>`
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`

        Returns:
            The added instance.
        """
        with wrap_sqlalchemy_exception():
            instance = await self._attach_to_session(data)
            await self._flush_or_commit(auto_commit=auto_commit)
            await self._refresh(instance, auto_refresh=auto_refresh)
            self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    async def add_many(
        self,
        data: list[ModelT],
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
    ) -> list[ModelT]:
        """Add Many `data` to the collection.

        Args:
            data: list of Instances to be added to the collection.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`

        Returns:
            The added instances.
        """
        with wrap_sqlalchemy_exception():
            self.session.add_all(data)
            await self._flush_or_commit(auto_commit=auto_commit)
            for datum in data:
                self._expunge(datum, auto_expunge=auto_expunge)
            return data

    async def delete(
        self,
        item_id: Any,
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
        id_attribute: str | InstrumentedAttribute | None = None,
    ) -> ModelT:
        """Delete instance identified by ``item_id``.

        Args:
            item_id: Identifier of instance to be deleted.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`
            id_attribute: Allows customization of the unique identifier to use for model fetching.
                Defaults to `id`, but can reference any surrogate or candidate key for the table.

        Returns:
            The deleted instance.

        Raises:
            NotFoundError: If no instance found identified by ``item_id``.
        """
        with wrap_sqlalchemy_exception():
            instance = await self.get(item_id, id_attribute=id_attribute)
            await self.session.delete(instance)
            await self._flush_or_commit(auto_commit=auto_commit)
            self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    async def delete_many(
        self,
        item_ids: list[Any],
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
        id_attribute: str | InstrumentedAttribute | None = None,
        chunk_size: int | None = None,
    ) -> list[ModelT]:
        """Delete instance identified by `item_id`.

        Args:
            item_ids: Identifier of instance to be deleted.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`
            id_attribute: Allows customization of the unique identifier to use for model fetching.
                Defaults to `id`, but can reference any surrogate or candidate key for the table.
            chunk_size: Allows customization of the ``insertmanyvalues_max_parameters`` setting for the driver.
                Defaults to `950` if left unset.

        Returns:
            The deleted instances.

        """

        with wrap_sqlalchemy_exception():
            id_attribute = get_instrumented_attr(
                self.model_type, id_attribute if id_attribute is not None else self.id_attribute
            )
            instances: list[ModelT] = []
            chunk_size = self._get_insertmanyvalues_max_parameters(chunk_size)
            for idx in range(0, len(item_ids), chunk_size):
                chunk = item_ids[idx : min(idx + chunk_size, len(item_ids))]
                if self._dialect.delete_executemany_returning:
                    instances.extend(
                        await self.session.scalars(
                            delete(self.model_type).where(id_attribute.in_(chunk)).returning(self.model_type)
                        )
                    )
                else:
                    instances.extend(await self.session.scalars(select(self.model_type).where(id_attribute.in_(chunk))))
                    await self.session.execute(delete(self.model_type).where(id_attribute.in_(chunk)))
            await self._flush_or_commit(auto_commit=auto_commit)
            for instance in instances:
                self._expunge(instance, auto_expunge=auto_expunge)
            return instances

    def _get_insertmanyvalues_max_parameters(self, chunk_size: int | None = None) -> int:
        return chunk_size if chunk_size is not None else DEFAULT_INSERTMANYVALUES_MAX_PARAMETERS

    async def exists(self, **kwargs: Any) -> bool:
        """Return true if the object specified by ``kwargs`` exists.

        Args:
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            True if the instance was found.  False if not found..

        """
        existing = await self.count(**kwargs)
        return existing > 0

    async def get(  # type: ignore[override]
        self,
        item_id: Any,
        auto_expunge: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        id_attribute: str | InstrumentedAttribute | None = None,
    ) -> ModelT:
        """Get instance identified by `item_id`.

        Args:
            item_id: Identifier of the instance to be retrieved.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            id_attribute: Allows customization of the unique identifier to use for model fetching.
                Defaults to `id`, but can reference any surrogate or candidate key for the table.

        Returns:
            The retrieved instance.

        Raises:
            NotFoundError: If no instance found identified by `item_id`.
        """
        with wrap_sqlalchemy_exception():
            id_attribute = id_attribute if id_attribute is not None else self.id_attribute
            statement = statement if statement is not None else self.statement
            statement = self._filter_select_by_kwargs(statement=statement, kwargs=[(id_attribute, item_id)])
            instance = (await self._execute(statement)).scalar_one_or_none()
            instance = self.check_not_found(instance)
            self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    async def get_one(
        self,
        auto_expunge: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> ModelT:
        """Get instance identified by ``kwargs``.

        Args:
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            The retrieved instance.

        Raises:
            NotFoundError: If no instance found identified by `item_id`.
        """
        with wrap_sqlalchemy_exception():
            statement = statement if statement is not None else self.statement
            statement = self._filter_select_by_kwargs(statement=statement, kwargs=kwargs)
            instance = (await self._execute(statement)).scalar_one_or_none()
            instance = self.check_not_found(instance)
            self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    async def get_one_or_none(
        self,
        auto_expunge: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> ModelT | None:
        """Get instance identified by ``kwargs`` or None if not found.

        Args:
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            The retrieved instance or None
        """
        with wrap_sqlalchemy_exception():
            statement = statement if statement is not None else self.statement
            statement = self._filter_select_by_kwargs(statement=statement, kwargs=kwargs)
            instance = (await self._execute(statement)).scalar_one_or_none()
            if instance:
                self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    async def get_or_create(
        self,
        match_fields: list[str] | str | None = None,
        upsert: bool = True,
        attribute_names: Iterable[str] | None = None,
        with_for_update: bool | None = None,
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
        auto_refresh: bool | None = None,
        **kwargs: Any,
    ) -> tuple[ModelT, bool]:
        """Get instance identified by ``kwargs`` or create if it doesn't exist.

        Args:
            match_fields: a list of keys to use to match the existing model.  When
                empty, all fields are matched.
            upsert: When using match_fields and actual model values differ from
                `kwargs`, perform an update operation on the model.
            attribute_names: an iterable of attribute names to pass into the ``update``
                method.
            with_for_update: indicating FOR UPDATE should be used, or may be a
                dictionary containing flags to indicate a more specific set of
                FOR UPDATE flags for the SELECT
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_refresh: Refresh object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_refresh <SQLAlchemyAsyncRepository>`
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            a tuple that includes the instance and whether it needed to be created.
            When using match_fields and actual model values differ from ``kwargs``, the
            model value will be updated.
        """
        match_fields = match_fields or self.match_fields
        if isinstance(match_fields, str):
            match_fields = [match_fields]
        if match_fields:
            match_filter = {
                field_name: kwargs.get(field_name, None)
                for field_name in match_fields
                if kwargs.get(field_name, None) is not None
            }
        else:
            match_filter = kwargs
        existing = await self.get_one_or_none(**match_filter)
        if not existing:
            return await self.add(self.model_type(**kwargs)), True  # pyright: ignore[reportGeneralTypeIssues]
        if upsert:
            for field_name, new_field_value in kwargs.items():
                field = getattr(existing, field_name, None)
                if field and field != new_field_value:
                    setattr(existing, field_name, new_field_value)
            existing = await self._attach_to_session(existing, strategy="merge")
            await self._flush_or_commit(auto_commit=auto_commit)
            await self._refresh(
                existing, attribute_names=attribute_names, with_for_update=with_for_update, auto_refresh=auto_refresh
            )
            self._expunge(existing, auto_expunge=auto_expunge)
        return existing, False

    async def count(
        self,
        *filters: FilterTypes,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> int:
        """Get the count of records returned by a query.

        Args:
            *filters: Types for specific filtering operations.
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query, ignoring pagination.
        """
        statement = statement if statement is not None else self.statement
        statement = statement.with_only_columns(
            sql_func.count(self.get_id_attribute_value(self.model_type)),
            maintain_column_froms=True,
        ).order_by(None)
        statement = self._apply_filters(*filters, apply_pagination=False, statement=statement)
        statement = self._filter_select_by_kwargs(statement, kwargs)
        results = await self._execute(statement)
        return results.scalar_one()  # type: ignore

    async def update(
        self,
        data: ModelT,
        attribute_names: Iterable[str] | None = None,
        with_for_update: bool | None = None,
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
        auto_refresh: bool | None = None,
        id_attribute: str | InstrumentedAttribute | None = None,
    ) -> ModelT:
        """Update instance with the attribute values present on `data`.

        Args:
            data: An instance that should have a value for `self.id_attribute` that
                exists in the collection.
            attribute_names: an iterable of attribute names to pass into the ``update``
                method.
            with_for_update: indicating FOR UPDATE should be used, or may be a
                dictionary containing flags to indicate a more specific set of
                FOR UPDATE flags for the SELECT
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_refresh: Refresh object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_refresh <SQLAlchemyAsyncRepository>`
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`
            id_attribute: Allows customization of the unique identifier to use for model fetching.
                Defaults to `id`, but can reference any surrogate or candidate key for the table.

        Returns:
            The updated instance.

        Raises:
            NotFoundError: If no instance found with same identifier as `data`.
        """
        with wrap_sqlalchemy_exception():
            item_id = self.get_id_attribute_value(
                data, id_attribute=id_attribute.key if isinstance(id_attribute, InstrumentedAttribute) else id_attribute
            )
            # this will raise for not found, and will put the item in the session
            await self.get(item_id, id_attribute=id_attribute)
            # this will merge the inbound data to the instance we just put in the session
            instance = await self._attach_to_session(data, strategy="merge")
            await self._flush_or_commit(auto_commit=auto_commit)
            await self._refresh(
                instance, attribute_names=attribute_names, with_for_update=with_for_update, auto_refresh=auto_refresh
            )
            self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    async def update_many(
        self,
        data: list[ModelT],
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
    ) -> list[ModelT]:
        """Update one or more instances with the attribute values present on `data`.

        This function has an optimized bulk update based on the configured SQL dialect:
        - For backends supporting `RETURNING` with `executemany`, a single bulk update with returning clause is executed.
        - For other backends, it does a bulk update and then returns the updated data after a refresh.

        Args:
            data: A list of instances to update.  Each should have a value for `self.id_attribute` that exists in the
                collection.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`

        Returns:
            The updated instances.

        Raises:
            NotFoundError: If no instance found with same identifier as `data`.
        """
        data_to_update: list[dict[str, Any]] = [v.to_dict() if isinstance(v, self.model_type) else v for v in data]  # type: ignore
        with wrap_sqlalchemy_exception():
            if self._dialect.update_executemany_returning and self._dialect.name != "oracle":
                instances = list(
                    await self.session.scalars(
                        update(self.model_type).returning(self.model_type),
                        cast("_CoreSingleExecuteParams", data_to_update),  # this is not correct but the only way
                        # currently to deal with an SQLAlchemy typing issue. See
                        # https://github.com/sqlalchemy/sqlalchemy/discussions/9925
                    )
                )
                await self._flush_or_commit(auto_commit=auto_commit)
                for instance in instances:
                    self._expunge(instance, auto_expunge=auto_expunge)
                return instances
            await self.session.execute(update(self.model_type), data_to_update)
            await self._flush_or_commit(auto_commit=auto_commit)
            return data

    async def list_and_count(
        self,
        *filters: FilterTypes,
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
        auto_refresh: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> tuple[list[ModelT], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_refresh: Refresh object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_refresh <SQLAlchemyAsyncRepository>`
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query, ignoring pagination.
        """
        if self._dialect.name in {"spanner", "spanner+spanner"}:
            return await self._list_and_count_basic(*filters, auto_expunge=auto_expunge, statement=statement, **kwargs)
        return await self._list_and_count_window(*filters, auto_expunge=auto_expunge, statement=statement, **kwargs)

    def _expunge(self, instance: ModelT, auto_expunge: bool | None) -> None:
        if auto_expunge is None:
            auto_expunge = self.auto_expunge

        return self.session.expunge(instance) if auto_expunge else None

    async def _flush_or_commit(self, auto_commit: bool | None) -> None:
        if auto_commit is None:
            auto_commit = self.auto_commit

        return await self.session.commit() if auto_commit else await self.session.flush()

    async def _refresh(
        self,
        instance: ModelT,
        auto_refresh: bool | None,
        attribute_names: Iterable[str] | None = None,
        with_for_update: bool | None = None,
    ) -> None:
        if auto_refresh is None:
            auto_refresh = self.auto_refresh

        return (
            await self.session.refresh(instance, attribute_names=attribute_names, with_for_update=with_for_update)
            if auto_refresh
            else None
        )

    async def _list_and_count_window(
        self,
        *filters: FilterTypes,
        auto_expunge: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> tuple[list[ModelT], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query using an analytical window function, ignoring pagination.
        """
        statement = statement if statement is not None else self.statement
        statement = statement.add_columns(over(sql_func.count(self.get_id_attribute_value(self.model_type))))
        statement = self._apply_filters(*filters, statement=statement)
        statement = self._filter_select_by_kwargs(statement, kwargs)
        with wrap_sqlalchemy_exception():
            result = await self._execute(statement)
            count: int = 0
            instances: list[ModelT] = []
            for i, (instance, count_value) in enumerate(result):
                self._expunge(instance, auto_expunge=auto_expunge)
                instances.append(instance)
                if i == 0:
                    count = count_value
            return instances, count

    async def _list_and_count_basic(
        self,
        *filters: FilterTypes,
        auto_expunge: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> tuple[list[ModelT], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query using 2 queries, ignoring pagination.
        """
        statement = statement if statement is not None else self.statement
        statement = self._apply_filters(*filters, statement=statement)
        statement = self._filter_select_by_kwargs(statement, kwargs)
        count_statement = statement.with_only_columns(
            sql_func.count(self.get_id_attribute_value(self.model_type)),
            maintain_column_froms=True,
        ).order_by(None)
        with wrap_sqlalchemy_exception():
            count_result = await self.session.execute(count_statement)
            count = count_result.scalar_one()
            result = await self._execute(statement)
            instances: list[ModelT] = []
            for (instance,) in result:
                self._expunge(instance, auto_expunge=auto_expunge)
                instances.append(instance)
            return instances, count

    async def upsert(
        self,
        data: ModelT,
        attribute_names: Iterable[str] | None = None,
        with_for_update: bool | None = None,
        auto_expunge: bool | None = None,
        auto_commit: bool | None = None,
        auto_refresh: bool | None = None,
    ) -> ModelT:
        """Update or create instance.

        Updates instance with the attribute values present on `data`, or creates a new instance if
        one doesn't exist.

        Args:
            data: Instance to update existing, or be created. Identifier used to determine if an
                existing instance exists is the value of an attribute on `data` named as value of
                `self.id_attribute`.
            attribute_names: an iterable of attribute names to pass into the ``update`` method.
            with_for_update: indicating FOR UPDATE should be used, or may be a
                dictionary containing flags to indicate a more specific set of
                FOR UPDATE flags for the SELECT
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_refresh: Refresh object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_refresh <SQLAlchemyAsyncRepository>`
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`

        Returns:
            The updated or created instance.

        Raises:
            NotFoundError: If no instance found with same identifier as `data`.
        """
        with wrap_sqlalchemy_exception():
            instance = await self._attach_to_session(data, strategy="merge")
            await self._flush_or_commit(auto_commit=auto_commit)
            await self._refresh(
                instance, attribute_names=attribute_names, with_for_update=with_for_update, auto_refresh=auto_refresh
            )
            self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    async def upsert_many(
        self,
        data: list[ModelT],
        attribute_names: Iterable[str] | None = None,
        with_for_update: bool | None = None,
        auto_expunge: bool | None = None,
        auto_commit: bool | None = None,
        auto_refresh: bool | None = None,
    ) -> list[ModelT]:
        """Update or create instance.

        Update instances with the attribute values present on `data`, or create a new instance if
        one doesn't exist.

        Args:
            data: Instance to update existing, or be created. Identifier used to determine if an
                existing instance exists is the value of an attribute on ``data`` named as value of
                :attr:`~litestar.contrib.repository.AbstractAsyncRepository.id_attribute`.
            attribute_names: an iterable of attribute names to pass into the ``update`` method.
            with_for_update: indicating FOR UPDATE should be used, or may be a dictionary containing flags to indicate a more specific set of FOR UPDATE flags for the SELECT
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_refresh: Refresh object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_refresh <SQLAlchemyAsyncRepository>`
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`

        Returns:
            The updated or created instance.

        Raises:
            NotFoundError: If no instance found with same identifier as ``data``.
        """
        instances = []
        with wrap_sqlalchemy_exception():
            for datum in data:
                instance = await self._attach_to_session(datum, strategy="merge")
                await self._flush_or_commit(auto_commit=auto_commit)
                await self._refresh(
                    instance,
                    attribute_names=attribute_names,
                    with_for_update=with_for_update,
                    auto_refresh=auto_refresh,
                )
                self._expunge(instance, auto_expunge=auto_expunge)
                instances.append(instance)
        return instances

    async def list(
        self,
        *filters: FilterTypes,
        auto_expunge: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> list[ModelT]:
        """Get a list of instances, optionally filtered.

        Args:
            *filters: Types for specific filtering operations.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Instance attribute value filters.

        Returns:
            The list of instances, after filtering applied.
        """
        statement = statement if statement is not None else self.statement
        statement = self._apply_filters(*filters, statement=statement)
        statement = self._filter_select_by_kwargs(statement, kwargs)

        with wrap_sqlalchemy_exception():
            result = await self._execute(statement)
            instances = list(result.scalars())
            for instance in instances:
                self._expunge(instance, auto_expunge=auto_expunge)
            return instances

    def filter_collection_by_kwargs(  # type:ignore[override]
        self, collection: SelectT, /, **kwargs: Any
    ) -> SelectT:
        """Filter the collection by kwargs.

        Args:
            collection: statement to filter
            **kwargs: key/value pairs such that objects remaining in the collection after filtering
                have the property that their attribute named `key` has value equal to `value`.
        """
        with wrap_sqlalchemy_exception():
            return collection.filter_by(**kwargs)

    @classmethod
    async def check_health(cls, session: AsyncSession) -> bool:
        """Perform a health check on the database.

        Args:
            session: through which we run a check statement

        Returns:
            ``True`` if healthy.
        """

        return (  # type:ignore[no-any-return]
            await session.execute(cls._get_health_check_statement(session))
        ).scalar_one() == 1

    @staticmethod
    def _get_health_check_statement(session: AsyncSession) -> TextClause:
        if session.bind and session.bind.dialect.name == "oracle":
            return text("SELECT 1 FROM DUAL")
        return text("SELECT 1")

    async def _attach_to_session(self, model: ModelT, strategy: Literal["add", "merge"] = "add") -> ModelT:
        """Attach detached instance to the session.

        Args:
            model: The instance to be attached to the session.
            strategy: How the instance should be attached.
                - "add": New instance added to session
                - "merge": Instance merged with existing, or new one added.

        Returns:
            Instance attached to the session - if `"merge"` strategy, may not be same instance
            that was provided.
        """
        if strategy == "add":
            self.session.add(model)
            return model
        if strategy == "merge":
            return await self.session.merge(model)
        raise ValueError("Unexpected value for `strategy`, must be `'add'` or `'merge'`")

    async def _execute(self, statement: Select[RowT]) -> Result[RowT]:
        return cast("Result[RowT]", await self.session.execute(statement))

    def _apply_limit_offset_pagination(self, limit: int, offset: int, statement: SelectT) -> SelectT:
        return statement.limit(limit).offset(offset)

    def _apply_filters(self, *filters: FilterTypes, apply_pagination: bool = True, statement: SelectT) -> SelectT:
        """Apply filters to a select statement.

        Args:
            *filters: filter types to apply to the query
            apply_pagination: applies pagination filters if true
            statement: select statement to apply filters

        Keyword Args:
            select: select to apply filters against

        Returns:
            The select with filters applied.
        """
        for filter_ in filters:
            if isinstance(filter_, LimitOffset):
                if apply_pagination:
                    statement = self._apply_limit_offset_pagination(filter_.limit, filter_.offset, statement=statement)
            elif isinstance(filter_, BeforeAfter):
                statement = self._filter_on_datetime_field(
                    field_name=filter_.field_name,
                    before=filter_.before,
                    after=filter_.after,
                    statement=statement,
                )
            elif isinstance(filter_, OnBeforeAfter):
                statement = self._filter_on_datetime_field(
                    field_name=filter_.field_name,
                    on_or_before=filter_.on_or_before,
                    on_or_after=filter_.on_or_after,
                    statement=statement,
                )

            elif isinstance(filter_, NotInCollectionFilter):
                statement = self._filter_not_in_collection(filter_.field_name, filter_.values, statement=statement)
            elif isinstance(filter_, CollectionFilter):
                statement = self._filter_in_collection(filter_.field_name, filter_.values, statement=statement)
            elif isinstance(filter_, OrderBy):
                statement = self._order_by(
                    statement,
                    filter_.field_name,
                    sort_desc=filter_.sort_order == "desc",
                )
            elif isinstance(filter_, SearchFilter):
                statement = self._filter_by_like(
                    statement, filter_.field_name, value=filter_.value, ignore_case=bool(filter_.ignore_case)
                )
            elif isinstance(filter_, NotInSearchFilter):
                statement = self._filter_by_not_like(
                    statement, filter_.field_name, value=filter_.value, ignore_case=bool(filter_.ignore_case)
                )
            else:
                raise RepositoryError(f"Unexpected filter: {filter_}")
        return statement

    def _filter_in_collection(self, field_name: str, values: abc.Collection[Any], statement: SelectT) -> SelectT:
        if not values:
            return statement
        return statement.where(getattr(self.model_type, field_name).in_(values))

    def _filter_not_in_collection(self, field_name: str, values: abc.Collection[Any], statement: SelectT) -> SelectT:
        if not values:
            return statement
        return statement.where(getattr(self.model_type, field_name).notin_(values))

    def _filter_on_datetime_field(
        self,
        field_name: str,
        statement: SelectT,
        before: datetime | None = None,
        after: datetime | None = None,
        on_or_before: datetime | None = None,
        on_or_after: datetime | None = None,
    ) -> SelectT:
        field = getattr(self.model_type, field_name)
        if before is not None:
            statement = statement.where(field < before)
        if after is not None:
            statement = statement.where(field > after)
        if on_or_before is not None:
            statement = statement.where(field <= on_or_before)
        if on_or_after is not None:
            statement = statement.where(field >= on_or_after)
        return statement

    def _filter_select_by_kwargs(
        self, statement: SelectT, kwargs: dict[Any, Any] | Iterable[tuple[Any, Any]]
    ) -> SelectT:
        for key, val in kwargs.items() if isinstance(kwargs, dict) else kwargs:
            statement = statement.where(get_instrumented_attr(self.model_type, key) == val)  # pyright: ignore
        return statement

    def _filter_by_like(
        self, statement: SelectT, field_name: str | InstrumentedAttribute, value: str, ignore_case: bool
    ) -> SelectT:
        field = get_instrumented_attr(self.model_type, field_name)
        search_text = f"%{value}%"
        return statement.where(field.ilike(search_text) if ignore_case else field.like(search_text))

    def _filter_by_not_like(self, statement: SelectT, field_name: str, value: str, ignore_case: bool) -> SelectT:
        field = getattr(self.model_type, field_name)
        search_text = f"%{value}%"
        return statement.where(field.not_ilike(search_text) if ignore_case else field.not_like(search_text))

    def _order_by(
        self, statement: SelectT, field_name: str | InstrumentedAttribute, sort_desc: bool = False
    ) -> SelectT:
        field = get_instrumented_attr(self.model_type, field_name)
        return statement.order_by(field.desc() if sort_desc else field.asc())


class SQLAlchemySyncRepository(AbstractSyncRepository[ModelT], Generic[ModelT]):
    """SQLAlchemy based implementation of the repository interface."""

    match_fields: list[str] | str | None = None
    id_attribute: str | InstrumentedAttribute

    def __init__(
        self,
        *,
        statement: Select[tuple[ModelT]] | None = None,
        session: Session,
        auto_expunge: bool = False,
        auto_refresh: bool = True,
        auto_commit: bool = False,
        **kwargs: Any,
    ) -> None:
        """Repository pattern for SQLAlchemy models.

        Args:
            statement: To facilitate customization of the underlying select query.
            session: Session managing the unit-of-work for the operation.
            auto_expunge: Remove object from session before returning.
            auto_refresh: Refresh object from session before returning.
            auto_commit: Commit objects before returning.
            **kwargs: Additional arguments.

        """
        super().__init__(**kwargs)
        self.auto_expunge = auto_expunge
        self.auto_refresh = auto_refresh
        self.auto_commit = auto_commit
        self.session = session
        self.statement = statement if statement is not None else select(self.model_type)
        if not self.session.bind:
            # this shouldn't actually ever happen, but we include it anyway to properly
            # narrow down the types
            raise ValueError("Session improperly configure")
        self._dialect = self.session.bind.dialect

    def add(
        self,
        data: ModelT,
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
        auto_refresh: bool | None = None,
    ) -> ModelT:
        """Add `data` to the collection.

        Args:
            data: Instance to be added to the collection.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_refresh: Refresh object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_refresh <SQLAlchemyAsyncRepository>`
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`

        Returns:
            The added instance.
        """
        with wrap_sqlalchemy_exception():
            instance = self._attach_to_session(data)
            self._flush_or_commit(auto_commit=auto_commit)
            self._refresh(instance, auto_refresh=auto_refresh)
            self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    def add_many(
        self,
        data: list[ModelT],
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
    ) -> list[ModelT]:
        """Add Many `data` to the collection.

        Args:
            data: list of Instances to be added to the collection.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`

        Returns:
            The added instances.
        """
        with wrap_sqlalchemy_exception():
            self.session.add_all(data)
            self._flush_or_commit(auto_commit=auto_commit)
            for datum in data:
                self._expunge(datum, auto_expunge=auto_expunge)
            return data

    def delete(
        self,
        item_id: Any,
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
        id_attribute: str | InstrumentedAttribute | None = None,
    ) -> ModelT:
        """Delete instance identified by ``item_id``.

        Args:
            item_id: Identifier of instance to be deleted.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`
            id_attribute: Allows customization of the unique identifier to use for model fetching.
                Defaults to `id`, but can reference any surrogate or candidate key for the table.

        Returns:
            The deleted instance.

        Raises:
            NotFoundError: If no instance found identified by ``item_id``.
        """
        with wrap_sqlalchemy_exception():
            instance = self.get(item_id, id_attribute=id_attribute)
            self.session.delete(instance)
            self._flush_or_commit(auto_commit=auto_commit)
            self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    def delete_many(
        self,
        item_ids: list[Any],
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
        id_attribute: str | InstrumentedAttribute | None = None,
        chunk_size: int | None = None,
    ) -> list[ModelT]:
        """Delete instance identified by `item_id`.

        Args:
            item_ids: Identifier of instance to be deleted.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`
            id_attribute: Allows customization of the unique identifier to use for model fetching.
                Defaults to `id`, but can reference any surrogate or candidate key for the table.
            chunk_size: Allows customization of the ``insertmanyvalues_max_parameters`` setting for the driver.
                Defaults to `950` if left unset.

        Returns:
            The deleted instances.

        """

        with wrap_sqlalchemy_exception():
            id_attribute = get_instrumented_attr(
                self.model_type, id_attribute if id_attribute is not None else self.id_attribute
            )
            instances: list[ModelT] = []
            chunk_size = self._get_insertmanyvalues_max_parameters(chunk_size)
            for idx in range(0, len(item_ids), chunk_size):
                chunk = item_ids[idx : min(idx + chunk_size, len(item_ids))]
                if self._dialect.delete_executemany_returning:
                    instances.extend(
                        self.session.scalars(
                            delete(self.model_type).where(id_attribute.in_(chunk)).returning(self.model_type)
                        )
                    )
                else:
                    instances.extend(self.session.scalars(select(self.model_type).where(id_attribute.in_(chunk))))
                    self.session.execute(delete(self.model_type).where(id_attribute.in_(chunk)))
            self._flush_or_commit(auto_commit=auto_commit)
            for instance in instances:
                self._expunge(instance, auto_expunge=auto_expunge)
            return instances

    def _get_insertmanyvalues_max_parameters(self, chunk_size: int | None = None) -> int:
        return chunk_size if chunk_size is not None else DEFAULT_INSERTMANYVALUES_MAX_PARAMETERS

    def exists(self, **kwargs: Any) -> bool:
        """Return true if the object specified by ``kwargs`` exists.

        Args:
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            True if the instance was found.  False if not found..

        """
        existing = self.count(**kwargs)
        return existing > 0

    def get(  # type: ignore[override]
        self,
        item_id: Any,
        auto_expunge: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        id_attribute: str | InstrumentedAttribute | None = None,
    ) -> ModelT:
        """Get instance identified by `item_id`.

        Args:
            item_id: Identifier of the instance to be retrieved.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            id_attribute: Allows customization of the unique identifier to use for model fetching.
                Defaults to `id`, but can reference any surrogate or candidate key for the table.

        Returns:
            The retrieved instance.

        Raises:
            NotFoundError: If no instance found identified by `item_id`.
        """
        with wrap_sqlalchemy_exception():
            id_attribute = id_attribute if id_attribute is not None else self.id_attribute
            statement = statement if statement is not None else self.statement
            statement = self._filter_select_by_kwargs(statement=statement, kwargs=[(id_attribute, item_id)])
            instance = (self._execute(statement)).scalar_one_or_none()
            instance = self.check_not_found(instance)
            self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    def get_one(
        self,
        auto_expunge: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> ModelT:
        """Get instance identified by ``kwargs``.

        Args:
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            The retrieved instance.

        Raises:
            NotFoundError: If no instance found identified by `item_id`.
        """
        with wrap_sqlalchemy_exception():
            statement = statement if statement is not None else self.statement
            statement = self._filter_select_by_kwargs(statement=statement, kwargs=kwargs)
            instance = (self._execute(statement)).scalar_one_or_none()
            instance = self.check_not_found(instance)
            self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    def get_one_or_none(
        self,
        auto_expunge: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> ModelT | None:
        """Get instance identified by ``kwargs`` or None if not found.

        Args:
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            The retrieved instance or None
        """
        with wrap_sqlalchemy_exception():
            statement = statement if statement is not None else self.statement
            statement = self._filter_select_by_kwargs(statement=statement, kwargs=kwargs)
            instance = (self._execute(statement)).scalar_one_or_none()
            if instance:
                self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    def get_or_create(
        self,
        match_fields: list[str] | str | None = None,
        upsert: bool = True,
        attribute_names: Iterable[str] | None = None,
        with_for_update: bool | None = None,
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
        auto_refresh: bool | None = None,
        **kwargs: Any,
    ) -> tuple[ModelT, bool]:
        """Get instance identified by ``kwargs`` or create if it doesn't exist.

        Args:
            match_fields: a list of keys to use to match the existing model.  When
                empty, all fields are matched.
            upsert: When using match_fields and actual model values differ from
                `kwargs`, perform an update operation on the model.
            attribute_names: an iterable of attribute names to pass into the ``update``
                method.
            with_for_update: indicating FOR UPDATE should be used, or may be a
                dictionary containing flags to indicate a more specific set of
                FOR UPDATE flags for the SELECT
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_refresh: Refresh object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_refresh <SQLAlchemyAsyncRepository>`
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            a tuple that includes the instance and whether it needed to be created.
            When using match_fields and actual model values differ from ``kwargs``, the
            model value will be updated.
        """
        match_fields = match_fields or self.match_fields
        if isinstance(match_fields, str):
            match_fields = [match_fields]
        if match_fields:
            match_filter = {
                field_name: kwargs.get(field_name, None)
                for field_name in match_fields
                if kwargs.get(field_name, None) is not None
            }
        else:
            match_filter = kwargs
        existing = self.get_one_or_none(**match_filter)
        if not existing:
            return self.add(self.model_type(**kwargs)), True  # pyright: ignore[reportGeneralTypeIssues]
        if upsert:
            for field_name, new_field_value in kwargs.items():
                field = getattr(existing, field_name, None)
                if field and field != new_field_value:
                    setattr(existing, field_name, new_field_value)
            existing = self._attach_to_session(existing, strategy="merge")
            self._flush_or_commit(auto_commit=auto_commit)
            self._refresh(
                existing, attribute_names=attribute_names, with_for_update=with_for_update, auto_refresh=auto_refresh
            )
            self._expunge(existing, auto_expunge=auto_expunge)
        return existing, False

    def count(
        self,
        *filters: FilterTypes,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> int:
        """Get the count of records returned by a query.

        Args:
            *filters: Types for specific filtering operations.
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query, ignoring pagination.
        """
        statement = statement if statement is not None else self.statement
        statement = statement.with_only_columns(
            sql_func.count(self.get_id_attribute_value(self.model_type)),
            maintain_column_froms=True,
        ).order_by(None)
        statement = self._apply_filters(*filters, apply_pagination=False, statement=statement)
        statement = self._filter_select_by_kwargs(statement, kwargs)
        results = self._execute(statement)
        return results.scalar_one()  # type: ignore

    def update(
        self,
        data: ModelT,
        attribute_names: Iterable[str] | None = None,
        with_for_update: bool | None = None,
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
        auto_refresh: bool | None = None,
        id_attribute: str | InstrumentedAttribute | None = None,
    ) -> ModelT:
        """Update instance with the attribute values present on `data`.

        Args:
            data: An instance that should have a value for `self.id_attribute` that
                exists in the collection.
            attribute_names: an iterable of attribute names to pass into the ``update``
                method.
            with_for_update: indicating FOR UPDATE should be used, or may be a
                dictionary containing flags to indicate a more specific set of
                FOR UPDATE flags for the SELECT
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_refresh: Refresh object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_refresh <SQLAlchemyAsyncRepository>`
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`
            id_attribute: Allows customization of the unique identifier to use for model fetching.
                Defaults to `id`, but can reference any surrogate or candidate key for the table.

        Returns:
            The updated instance.

        Raises:
            NotFoundError: If no instance found with same identifier as `data`.
        """
        with wrap_sqlalchemy_exception():
            item_id = self.get_id_attribute_value(
                data, id_attribute=id_attribute.key if isinstance(id_attribute, InstrumentedAttribute) else id_attribute
            )
            # this will raise for not found, and will put the item in the session
            self.get(item_id, id_attribute=id_attribute)
            # this will merge the inbound data to the instance we just put in the session
            instance = self._attach_to_session(data, strategy="merge")
            self._flush_or_commit(auto_commit=auto_commit)
            self._refresh(
                instance, attribute_names=attribute_names, with_for_update=with_for_update, auto_refresh=auto_refresh
            )
            self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    def update_many(
        self,
        data: list[ModelT],
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
    ) -> list[ModelT]:
        """Update one or more instances with the attribute values present on `data`.

        This function has an optimized bulk update based on the configured SQL dialect:
        - For backends supporting `RETURNING` with `executemany`, a single bulk update with returning clause is executed.
        - For other backends, it does a bulk update and then returns the updated data after a refresh.

        Args:
            data: A list of instances to update.  Each should have a value for `self.id_attribute` that exists in the
                collection.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`

        Returns:
            The updated instances.

        Raises:
            NotFoundError: If no instance found with same identifier as `data`.
        """
        data_to_update: list[dict[str, Any]] = [v.to_dict() if isinstance(v, self.model_type) else v for v in data]  # type: ignore
        with wrap_sqlalchemy_exception():
            if self._dialect.update_executemany_returning and self._dialect.name != "oracle":
                instances = list(
                    self.session.scalars(
                        update(self.model_type).returning(self.model_type),
                        cast("_CoreSingleExecuteParams", data_to_update),  # this is not correct but the only way
                        # currently to deal with an SQLAlchemy typing issue. See
                        # https://github.com/sqlalchemy/sqlalchemy/discussions/9925
                    )
                )
                self._flush_or_commit(auto_commit=auto_commit)
                for instance in instances:
                    self._expunge(instance, auto_expunge=auto_expunge)
                return instances
            self.session.execute(update(self.model_type), data_to_update)
            self._flush_or_commit(auto_commit=auto_commit)
            return data

    def list_and_count(
        self,
        *filters: FilterTypes,
        auto_commit: bool | None = None,
        auto_expunge: bool | None = None,
        auto_refresh: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> tuple[list[ModelT], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_refresh: Refresh object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_refresh <SQLAlchemyAsyncRepository>`
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query, ignoring pagination.
        """
        if self._dialect.name in {"spanner", "spanner+spanner"}:
            return self._list_and_count_basic(*filters, auto_expunge=auto_expunge, statement=statement, **kwargs)
        return self._list_and_count_window(*filters, auto_expunge=auto_expunge, statement=statement, **kwargs)

    def _expunge(self, instance: ModelT, auto_expunge: bool | None) -> None:
        if auto_expunge is None:
            auto_expunge = self.auto_expunge

        return self.session.expunge(instance) if auto_expunge else None

    def _flush_or_commit(self, auto_commit: bool | None) -> None:
        if auto_commit is None:
            auto_commit = self.auto_commit

        return self.session.commit() if auto_commit else self.session.flush()

    def _refresh(
        self,
        instance: ModelT,
        auto_refresh: bool | None,
        attribute_names: Iterable[str] | None = None,
        with_for_update: bool | None = None,
    ) -> None:
        if auto_refresh is None:
            auto_refresh = self.auto_refresh

        return (
            self.session.refresh(instance, attribute_names=attribute_names, with_for_update=with_for_update)
            if auto_refresh
            else None
        )

    def _list_and_count_window(
        self,
        *filters: FilterTypes,
        auto_expunge: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> tuple[list[ModelT], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query using an analytical window function, ignoring pagination.
        """
        statement = statement if statement is not None else self.statement
        statement = statement.add_columns(over(sql_func.count(self.get_id_attribute_value(self.model_type))))
        statement = self._apply_filters(*filters, statement=statement)
        statement = self._filter_select_by_kwargs(statement, kwargs)
        with wrap_sqlalchemy_exception():
            result = self._execute(statement)
            count: int = 0
            instances: list[ModelT] = []
            for i, (instance, count_value) in enumerate(result):
                self._expunge(instance, auto_expunge=auto_expunge)
                instances.append(instance)
                if i == 0:
                    count = count_value
            return instances, count

    def _list_and_count_basic(
        self,
        *filters: FilterTypes,
        auto_expunge: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> tuple[list[ModelT], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query using 2 queries, ignoring pagination.
        """
        statement = statement if statement is not None else self.statement
        statement = self._apply_filters(*filters, statement=statement)
        statement = self._filter_select_by_kwargs(statement, kwargs)
        count_statement = statement.with_only_columns(
            sql_func.count(self.get_id_attribute_value(self.model_type)),
            maintain_column_froms=True,
        ).order_by(None)
        with wrap_sqlalchemy_exception():
            count_result = self.session.execute(count_statement)
            count = count_result.scalar_one()
            result = self._execute(statement)
            instances: list[ModelT] = []
            for (instance,) in result:
                self._expunge(instance, auto_expunge=auto_expunge)
                instances.append(instance)
            return instances, count

    def upsert(
        self,
        data: ModelT,
        attribute_names: Iterable[str] | None = None,
        with_for_update: bool | None = None,
        auto_expunge: bool | None = None,
        auto_commit: bool | None = None,
        auto_refresh: bool | None = None,
    ) -> ModelT:
        """Update or create instance.

        Updates instance with the attribute values present on `data`, or creates a new instance if
        one doesn't exist.

        Args:
            data: Instance to update existing, or be created. Identifier used to determine if an
                existing instance exists is the value of an attribute on `data` named as value of
                `self.id_attribute`.
            attribute_names: an iterable of attribute names to pass into the ``update`` method.
            with_for_update: indicating FOR UPDATE should be used, or may be a
                dictionary containing flags to indicate a more specific set of
                FOR UPDATE flags for the SELECT
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_refresh: Refresh object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_refresh <SQLAlchemyAsyncRepository>`
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`

        Returns:
            The updated or created instance.

        Raises:
            NotFoundError: If no instance found with same identifier as `data`.
        """
        with wrap_sqlalchemy_exception():
            instance = self._attach_to_session(data, strategy="merge")
            self._flush_or_commit(auto_commit=auto_commit)
            self._refresh(
                instance, attribute_names=attribute_names, with_for_update=with_for_update, auto_refresh=auto_refresh
            )
            self._expunge(instance, auto_expunge=auto_expunge)
            return instance

    def upsert_many(
        self,
        data: list[ModelT],
        attribute_names: Iterable[str] | None = None,
        with_for_update: bool | None = None,
        auto_expunge: bool | None = None,
        auto_commit: bool | None = None,
        auto_refresh: bool | None = None,
    ) -> list[ModelT]:
        """Update or create instance.

        Update instances with the attribute values present on `data`, or create a new instance if
        one doesn't exist.

        Args:
            data: Instance to update existing, or be created. Identifier used to determine if an
                existing instance exists is the value of an attribute on ``data`` named as value of
                :attr:`~litestar.contrib.repository.AbstractAsyncRepository.id_attribute`.
            attribute_names: an iterable of attribute names to pass into the ``update`` method.
            with_for_update: indicating FOR UPDATE should be used, or may be a dictionary containing flags to indicate a more specific set of FOR UPDATE flags for the SELECT
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`.
            auto_refresh: Refresh object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_refresh <SQLAlchemyAsyncRepository>`
            auto_commit: Commit objects before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_commit <SQLAlchemyAsyncRepository>`

        Returns:
            The updated or created instance.

        Raises:
            NotFoundError: If no instance found with same identifier as ``data``.
        """
        instances = []
        with wrap_sqlalchemy_exception():
            for datum in data:
                instance = self._attach_to_session(datum, strategy="merge")
                self._flush_or_commit(auto_commit=auto_commit)
                self._refresh(
                    instance,
                    attribute_names=attribute_names,
                    with_for_update=with_for_update,
                    auto_refresh=auto_refresh,
                )
                self._expunge(instance, auto_expunge=auto_expunge)
                instances.append(instance)
        return instances

    def list(
        self,
        *filters: FilterTypes,
        auto_expunge: bool | None = None,
        statement: Select[tuple[ModelT]] | None = None,
        **kwargs: Any,
    ) -> list[ModelT]:
        """Get a list of instances, optionally filtered.

        Args:
            *filters: Types for specific filtering operations.
            auto_expunge: Remove object from session before returning. Defaults to
                :class:`SQLAlchemyAsyncRepository.auto_expunge <SQLAlchemyAsyncRepository>`
            statement: To facilitate customization of the underlying select query.
                Defaults to :class:`SQLAlchemyAsyncRepository.statement <SQLAlchemyAsyncRepository>`
            **kwargs: Instance attribute value filters.

        Returns:
            The list of instances, after filtering applied.
        """
        statement = statement if statement is not None else self.statement
        statement = self._apply_filters(*filters, statement=statement)
        statement = self._filter_select_by_kwargs(statement, kwargs)

        with wrap_sqlalchemy_exception():
            result = self._execute(statement)
            instances = list(result.scalars())
            for instance in instances:
                self._expunge(instance, auto_expunge=auto_expunge)
            return instances

    def filter_collection_by_kwargs(  # type:ignore[override]
        self, collection: SelectT, /, **kwargs: Any
    ) -> SelectT:
        """Filter the collection by kwargs.

        Args:
            collection: statement to filter
            **kwargs: key/value pairs such that objects remaining in the collection after filtering
                have the property that their attribute named `key` has value equal to `value`.
        """
        with wrap_sqlalchemy_exception():
            return collection.filter_by(**kwargs)

    @classmethod
    def check_health(cls, session: Session) -> bool:
        """Perform a health check on the database.

        Args:
            session: through which we run a check statement

        Returns:
            ``True`` if healthy.
        """

        return (  # type:ignore[no-any-return]
            session.execute(cls._get_health_check_statement(session))
        ).scalar_one() == 1

    @staticmethod
    def _get_health_check_statement(session: Session) -> TextClause:
        if session.bind and session.bind.dialect.name == "oracle":
            return text("SELECT 1 FROM DUAL")
        return text("SELECT 1")

    def _attach_to_session(self, model: ModelT, strategy: Literal["add", "merge"] = "add") -> ModelT:
        """Attach detached instance to the session.

        Args:
            model: The instance to be attached to the session.
            strategy: How the instance should be attached.
                - "add": New instance added to session
                - "merge": Instance merged with existing, or new one added.

        Returns:
            Instance attached to the session - if `"merge"` strategy, may not be same instance
            that was provided.
        """
        if strategy == "add":
            self.session.add(model)
            return model
        if strategy == "merge":
            return self.session.merge(model)
        raise ValueError("Unexpected value for `strategy`, must be `'add'` or `'merge'`")

    def _execute(self, statement: Select[RowT]) -> Result[RowT]:
        return cast("Result[RowT]", self.session.execute(statement))

    def _apply_limit_offset_pagination(self, limit: int, offset: int, statement: SelectT) -> SelectT:
        return statement.limit(limit).offset(offset)

    def _apply_filters(self, *filters: FilterTypes, apply_pagination: bool = True, statement: SelectT) -> SelectT:
        """Apply filters to a select statement.

        Args:
            *filters: filter types to apply to the query
            apply_pagination: applies pagination filters if true
            statement: select statement to apply filters

        Keyword Args:
            select: select to apply filters against

        Returns:
            The select with filters applied.
        """
        for filter_ in filters:
            if isinstance(filter_, LimitOffset):
                if apply_pagination:
                    statement = self._apply_limit_offset_pagination(filter_.limit, filter_.offset, statement=statement)
            elif isinstance(filter_, BeforeAfter):
                statement = self._filter_on_datetime_field(
                    field_name=filter_.field_name,
                    before=filter_.before,
                    after=filter_.after,
                    statement=statement,
                )
            elif isinstance(filter_, OnBeforeAfter):
                statement = self._filter_on_datetime_field(
                    field_name=filter_.field_name,
                    on_or_before=filter_.on_or_before,
                    on_or_after=filter_.on_or_after,
                    statement=statement,
                )

            elif isinstance(filter_, NotInCollectionFilter):
                statement = self._filter_not_in_collection(filter_.field_name, filter_.values, statement=statement)
            elif isinstance(filter_, CollectionFilter):
                statement = self._filter_in_collection(filter_.field_name, filter_.values, statement=statement)
            elif isinstance(filter_, OrderBy):
                statement = self._order_by(
                    statement,
                    filter_.field_name,
                    sort_desc=filter_.sort_order == "desc",
                )
            elif isinstance(filter_, SearchFilter):
                statement = self._filter_by_like(
                    statement, filter_.field_name, value=filter_.value, ignore_case=bool(filter_.ignore_case)
                )
            elif isinstance(filter_, NotInSearchFilter):
                statement = self._filter_by_not_like(
                    statement, filter_.field_name, value=filter_.value, ignore_case=bool(filter_.ignore_case)
                )
            else:
                raise RepositoryError(f"Unexpected filter: {filter_}")
        return statement

    def _filter_in_collection(self, field_name: str, values: abc.Collection[Any], statement: SelectT) -> SelectT:
        if not values:
            return statement
        return statement.where(getattr(self.model_type, field_name).in_(values))

    def _filter_not_in_collection(self, field_name: str, values: abc.Collection[Any], statement: SelectT) -> SelectT:
        if not values:
            return statement
        return statement.where(getattr(self.model_type, field_name).notin_(values))

    def _filter_on_datetime_field(
        self,
        field_name: str,
        statement: SelectT,
        before: datetime | None = None,
        after: datetime | None = None,
        on_or_before: datetime | None = None,
        on_or_after: datetime | None = None,
    ) -> SelectT:
        field = getattr(self.model_type, field_name)
        if before is not None:
            statement = statement.where(field < before)
        if after is not None:
            statement = statement.where(field > after)
        if on_or_before is not None:
            statement = statement.where(field <= on_or_before)
        if on_or_after is not None:
            statement = statement.where(field >= on_or_after)
        return statement

    def _filter_select_by_kwargs(
        self, statement: SelectT, kwargs: dict[Any, Any] | Iterable[tuple[Any, Any]]
    ) -> SelectT:
        for key, val in kwargs.items() if isinstance(kwargs, dict) else kwargs:
            statement = statement.where(get_instrumented_attr(self.model_type, key) == val)  # pyright: ignore
        return statement

    def _filter_by_like(
        self, statement: SelectT, field_name: str | InstrumentedAttribute, value: str, ignore_case: bool
    ) -> SelectT:
        field = get_instrumented_attr(self.model_type, field_name)
        search_text = f"%{value}%"
        return statement.where(field.ilike(search_text) if ignore_case else field.like(search_text))

    def _filter_by_not_like(self, statement: SelectT, field_name: str, value: str, ignore_case: bool) -> SelectT:
        field = getattr(self.model_type, field_name)
        search_text = f"%{value}%"
        return statement.where(field.not_ilike(search_text) if ignore_case else field.not_like(search_text))

    def _order_by(
        self, statement: SelectT, field_name: str | InstrumentedAttribute, sort_desc: bool = False
    ) -> SelectT:
        field = get_instrumented_attr(self.model_type, field_name)
        return statement.order_by(field.desc() if sort_desc else field.asc())
