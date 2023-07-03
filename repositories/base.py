
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
from typing import Any, Generic, Literal, Tuple, TypeAlias, TypeVar, cast

from sqlalchemy import Select, delete
from sqlalchemy import func as sql_func
from sqlalchemy import over, select, text, update
from sqlalchemy.engine import Result
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

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
class CollectionFilter(Generic[T]):
    """Data required to construct a ``WHERE ... IN (...)`` clause."""

    field_name: str
    """Name of the model attribute to filter on."""
    values: abc.Collection[T]
    """Values for ``IN`` clause."""


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


FilterTypes: TypeAlias = BeforeAfter | CollectionFilter[Any] | LimitOffset | OrderBy | SearchFilter
"""Aggregate type alias of the types supported for collection filtering."""


class AbstractAsyncRepository(Generic[T], metaclass=ABCMeta):
    """Interface for persistent data interaction."""

    model_type: type[T]
    """Type of object represented by the repository."""
    id_attribute = "id"
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
    def get_id_attribute_value(cls, item: T | type[T]) -> Any:
        """Get value of attribute named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` on ``item``.

        Args:
            item: Anything that should have an attribute named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` value.

        Returns:
            The value of attribute on ``item`` named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>`.
        """
        return getattr(item, cls.id_attribute)

    @classmethod
    def set_id_attribute_value(cls, item_id: Any, item: T) -> T:
        """Return the ``item`` after the ID is set to the appropriate attribute.

        Args:
            item_id: Value of ID to be set on instance
            item: Anything that should have an attribute named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` value.

        Returns:
            Item with ``item_id`` set to :attr:`id_attribute <AbstractAsyncRepository.id_attribute>`
        """
        setattr(item, cls.id_attribute, item_id)
        return item


class AbstractSyncRepository(Generic[T], metaclass=ABCMeta):
    """Interface for persistent data interaction."""

    model_type: type[T]
    """Type of object represented by the repository."""
    id_attribute = "id"
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
    def get_id_attribute_value(cls, item: T | type[T]) -> Any:
        """Get value of attribute named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` on ``item``.

        Args:
            item: Anything that should have an attribute named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` value.

        Returns:
            The value of attribute on ``item`` named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>`.
        """
        return getattr(item, cls.id_attribute)

    @classmethod
    def set_id_attribute_value(cls, item_id: Any, item: T) -> T:
        """Return the ``item`` after the ID is set to the appropriate attribute.

        Args:
            item_id: Value of ID to be set on instance
            item: Anything that should have an attribute named as :attr:`id_attribute <AbstractAsyncRepository.id_attribute>` value.

        Returns:
            Item with ``item_id`` set to :attr:`id_attribute <AbstractAsyncRepository.id_attribute>`
        """
        setattr(item, cls.id_attribute, item_id)
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


class SQLAlchemyAsyncRepository(AbstractAsyncRepository[ModelT], Generic[ModelT]):
    """SQLAlchemy based implementation of the repository interface."""

    match_fields: list[str] | str | None = None

    def __init__(self, *, statement: Select[tuple[ModelT]] | None = None, session: AsyncSession, **kwargs: Any) -> None:
        """Repository pattern for SQLAlchemy models.

        Args:
            statement: To facilitate customization of the underlying select query.
            session: Session managing the unit-of-work for the operation.
            **kwargs: Additional arguments.

        """
        super().__init__(**kwargs)
        self.session = session
        self.statement = statement if statement is not None else select(self.model_type)
        if not self.session.bind:
            # this shouldn't actually ever happen, but we include it anyway to properly
            # narrow down the types
            raise ValueError("Session improperly configure")
        self._dialect = self.session.bind.dialect

    async def add(self, data: ModelT) -> ModelT:
        """Add `data` to the collection.

        Args:
            data: Instance to be added to the collection.

        Returns:
            The added instance.
        """
        with wrap_sqlalchemy_exception():
            instance = await self._attach_to_session(data)
            await self.session.flush()
            await self.session.refresh(instance)
            self.session.expunge(instance)
            return instance

    async def add_many(self, data: list[ModelT]) -> list[ModelT]:
        """Add Many `data` to the collection.

        Args:
            data: list of Instances to be added to the collection.


        Returns:
            The added instances.
        """
        with wrap_sqlalchemy_exception():
            self.session.add_all(data)
            await self.session.flush()
            for datum in data:
                self.session.expunge(datum)
            return data

    async def delete(self, item_id: Any) -> ModelT:
        """Delete instance identified by ``item_id``.

        Args:
            item_id: Identifier of instance to be deleted.

        Returns:
            The deleted instance.

        Raises:
            NotFoundError: If no instance found identified by ``item_id``.
        """
        with wrap_sqlalchemy_exception():
            instance = await self.get(item_id)
            await self.session.delete(instance)
            await self.session.flush()
            self.session.expunge(instance)
            return instance

    async def delete_many(self, item_ids: list[Any]) -> list[ModelT]:
        """Delete instance identified by `item_id`.

        Args:
            item_ids: Identifier of instance to be deleted.

        Returns:
            The deleted instances.

        """
        with wrap_sqlalchemy_exception():
            instances: list[ModelT] = []
            chunk_size = 450
            for idx in range(0, len(item_ids), chunk_size):
                chunk = item_ids[idx : min(idx + chunk_size, len(item_ids))]
                if self._dialect.delete_executemany_returning:
                    instances.extend(
                        await self.session.scalars(
                            delete(self.model_type)
                            .where(getattr(self.model_type, self.id_attribute).in_(chunk))
                            .returning(self.model_type)
                        )
                    )
                else:
                    instances.extend(
                        await self.session.scalars(
                            select(self.model_type).where(getattr(self.model_type, self.id_attribute).in_(chunk))
                        )
                    )
                    await self.session.execute(
                        delete(self.model_type).where(getattr(self.model_type, self.id_attribute).in_(chunk))
                    )
            await self.session.flush()
            for instance in instances:
                self.session.expunge(instance)
            return instances

    async def exists(self, **kwargs: Any) -> bool:
        """Return true if the object specified by ``kwargs`` exists.

        Args:
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            True if the instance was found.  False if not found..

        """
        existing = await self.count(**kwargs)
        return existing > 0

    async def get(self, item_id: Any, **kwargs: Any) -> ModelT:
        """Get instance identified by `item_id`.

        Args:
            item_id: Identifier of the instance to be retrieved.
            **kwargs: Additional parameters

        Returns:
            The retrieved instance.

        Raises:
            NotFoundError: If no instance found identified by `item_id`.
        """
        with wrap_sqlalchemy_exception():
            statement = kwargs.pop("statement", self.statement)
            statement = self._filter_select_by_kwargs(statement=statement, **{self.id_attribute: item_id})
            instance = (await self._execute(statement)).scalar_one_or_none()
            instance = self.check_not_found(instance)
            self.session.expunge(instance)
            return instance

    async def get_one(self, **kwargs: Any) -> ModelT:
        """Get instance identified by ``kwargs``.

        Args:
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            The retrieved instance.

        Raises:
            NotFoundError: If no instance found identified by `item_id`.
        """
        with wrap_sqlalchemy_exception():
            statement = kwargs.pop("statement", self.statement)
            statement = self._filter_select_by_kwargs(statement=statement, **kwargs)
            instance = (await self._execute(statement)).scalar_one_or_none()
            instance = self.check_not_found(instance)
            self.session.expunge(instance)
            return instance

    async def get_one_or_none(self, **kwargs: Any) -> ModelT | None:
        """Get instance identified by ``kwargs`` or None if not found.

        Args:
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            The retrieved instance or None
        """
        with wrap_sqlalchemy_exception():
            statement = kwargs.pop("statement", self.statement)
            statement = self._filter_select_by_kwargs(statement=statement, **kwargs)
            instance = (await self._execute(statement)).scalar_one_or_none()
            if instance:
                self.session.expunge(instance)
            return instance  # type: ignore

    async def get_or_create(
        self, match_fields: list[str] | str | None = None, upsert: bool = True, **kwargs: Any
    ) -> tuple[ModelT, bool]:
        """Get instance identified by ``kwargs`` or create if it doesn't exist.

        Args:
            match_fields: a list of keys to use to match the existing model.  When empty, all fields are matched.
            upsert: When using match_fields and actual model values differ from `kwargs`, perform an update operation on the model.
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            a tuple that includes the instance and whether or not it needed to be created.  When using match_fields and actual model values differ from `kwargs`, the model value will be updated.
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
            await self.session.flush()
            await self.session.refresh(existing)
            self.session.expunge(existing)
        return existing, False

    async def count(self, *filters: FilterTypes, **kwargs: Any) -> int:
        """Get the count of records returned by a query.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query, ignoring pagination.
        """
        statement = kwargs.pop("statement", self.statement)
        statement = statement.with_only_columns(
            sql_func.count(self.get_id_attribute_value(self.model_type)),
            maintain_column_froms=True,
        ).order_by(None)
        statement = self._apply_filters(*filters, apply_pagination=False, statement=statement)
        statement = self._filter_select_by_kwargs(statement, **kwargs)
        results = await self._execute(statement)
        return results.scalar_one()  # type: ignore

    async def update(self, data: ModelT) -> ModelT:
        """Update instance with the attribute values present on `data`.

        Args:
            data: An instance that should have a value for `self.id_attribute` that exists in the
                collection.

        Returns:
            The updated instance.

        Raises:
            NotFoundError: If no instance found with same identifier as `data`.
        """
        with wrap_sqlalchemy_exception():
            item_id = self.get_id_attribute_value(data)
            # this will raise for not found, and will put the item in the session
            await self.get(item_id)
            # this will merge the inbound data to the instance we just put in the session
            instance = await self._attach_to_session(data, strategy="merge")
            await self.session.flush()
            await self.session.refresh(instance)
            self.session.expunge(instance)
            return instance

    async def update_many(self, data: list[ModelT]) -> list[ModelT]:
        """Update one or more instances with the attribute values present on `data`.

        This function has an optimized bulk insert based on the configured SQL dialect:
        - For backends supporting `RETURNING` with `executemany`, a single bulk insert with returning clause is executed.
        - For other backends, it does a bulk insert and then selects the inserted records

        Args:
            data: A list of instances to update.  Each should have a value for `self.id_attribute` that exists in the
                collection.

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
                await self.session.flush()
                for instance in instances:
                    self.session.expunge(instance)
                return instances
            await self.session.execute(
                update(self.model_type),
                data_to_update,
            )
            await self.session.flush()
            return data

    async def list_and_count(
        self,
        *filters: FilterTypes,
        **kwargs: Any,
    ) -> tuple[list[ModelT], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query, ignoring pagination.
        """
        if self._dialect.name in {"spanner", "spanner+spanner"}:
            return await self._list_and_count_basic(*filters, **kwargs)
        return await self._list_and_count_window(*filters, **kwargs)

    async def _list_and_count_window(
        self,
        *filters: FilterTypes,
        **kwargs: Any,
    ) -> tuple[list[ModelT], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query using an analytical window function, ignoring pagination.
        """
        statement = kwargs.pop("statement", self.statement)
        statement = statement.add_columns(over(sql_func.count(self.get_id_attribute_value(self.model_type))))
        statement = self._apply_filters(*filters, statement=statement)
        statement = self._filter_select_by_kwargs(statement, **kwargs)
        with wrap_sqlalchemy_exception():
            result = await self._execute(statement)
            count: int = 0
            instances: list[ModelT] = []
            for i, (instance, count_value) in enumerate(result):
                self.session.expunge(instance)
                instances.append(instance)
                if i == 0:
                    count = count_value
            return instances, count

    async def _list_and_count_basic(
        self,
        *filters: FilterTypes,
        **kwargs: Any,
    ) -> tuple[list[ModelT], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query using 2 queries, ignoring pagination.
        """
        statement = kwargs.pop("statement", self.statement)
        statement = self._apply_filters(*filters, statement=statement)
        statement = self._filter_select_by_kwargs(statement, **kwargs)
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
                self.session.expunge(instance)
                instances.append(instance)
            return instances, count

    async def list(self, *filters: FilterTypes, **kwargs: Any) -> list[ModelT]:
        """Get a list of instances, optionally filtered.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            The list of instances, after filtering applied.
        """
        statement = kwargs.pop("statement", self.statement)
        statement = self._apply_filters(*filters, statement=statement)
        statement = self._filter_select_by_kwargs(statement, **kwargs)

        with wrap_sqlalchemy_exception():
            result = await self._execute(statement)
            instances = list(result.scalars())
            for instance in instances:
                self.session.expunge(instance)
            return instances

    async def upsert(self, data: ModelT) -> ModelT:
        """Update or create instance.

        Updates instance with the attribute values present on `data`, or creates a new instance if
        one doesn't exist.

        Args:
            data: Instance to update existing, or be created. Identifier used to determine if an
                existing instance exists is the value of an attribute on `data` named as value of
                `self.id_attribute`.

        Returns:
            The updated or created instance.

        Raises:
            NotFoundError: If no instance found with same identifier as `data`.
        """
        with wrap_sqlalchemy_exception():
            instance = await self._attach_to_session(data, strategy="merge")
            await self.session.flush()
            await self.session.refresh(instance)
            self.session.expunge(instance)
            return instance

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
            `True` if healthy.
        """
        return (  # type:ignore[no-any-return]  # pragma: no cover
            await session.execute(text("SELECT 1"))
        ).scalar_one() == 1

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
                    filter_.field_name, filter_.before, filter_.after, statement=statement
                )
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
            else:
                raise RepositoryError(f"Unexpected filter: {filter_}")
        return statement

    def _filter_in_collection(self, field_name: str, values: abc.Collection[Any], statement: SelectT) -> SelectT:
        if not values:
            return statement
        return statement.where(getattr(self.model_type, field_name).in_(values))

    def _filter_on_datetime_field(
        self, field_name: str, before: datetime | None, after: datetime | None, statement: SelectT
    ) -> SelectT:
        field = getattr(self.model_type, field_name)
        if before is not None:
            statement = statement.where(field < before)
        if after is not None:
            statement = statement.where(field > after)
        return statement

    def _filter_select_by_kwargs(self, statement: SelectT, **kwargs: Any) -> SelectT:
        for key, val in kwargs.items():
            statement = statement.where(getattr(self.model_type, key) == val)
        return statement

    def _filter_by_like(self, statement: SelectT, field_name: str, value: str, ignore_case: bool) -> SelectT:
        field = getattr(self.model_type, field_name)
        search_text = f"%{value}%"
        return statement.where(field.ilike(search_text) if ignore_case else field.like(search_text))

    def _order_by(self, statement: SelectT, field_name: str, sort_desc: bool = False) -> SelectT:
        field = getattr(self.model_type, field_name)
        return statement.order_by(field.desc() if sort_desc else field.asc())


class SQLAlchemySyncRepository(AbstractSyncRepository[ModelT], Generic[ModelT]):
    """SQLAlchemy based implementation of the repository interface."""

    match_fields: list[str] | str | None = None

    def __init__(self, *, statement: Select[tuple[ModelT]] | None = None, session: Session, **kwargs: Any) -> None:
        """Repository pattern for SQLAlchemy models.

        Args:
            statement: To facilitate customization of the underlying select query.
            session: Session managing the unit-of-work for the operation.
            **kwargs: Additional arguments.

        """
        super().__init__(**kwargs)
        self.session = session
        self.statement = statement if statement is not None else select(self.model_type)
        if not self.session.bind:
            # this shouldn't actually ever happen, but we include it anyway to properly
            # narrow down the types
            raise ValueError("Session improperly configure")
        self._dialect = self.session.bind.dialect

    def add(self, data: ModelT) -> ModelT:
        """Add `data` to the collection.

        Args:
            data: Instance to be added to the collection.

        Returns:
            The added instance.
        """
        with wrap_sqlalchemy_exception():
            instance = self._attach_to_session(data)
            self.session.flush()
            self.session.refresh(instance)
            self.session.expunge(instance)
            return instance

    def add_many(self, data: list[ModelT]) -> list[ModelT]:
        """Add Many `data` to the collection.

        Args:
            data: list of Instances to be added to the collection.


        Returns:
            The added instances.
        """
        with wrap_sqlalchemy_exception():
            self.session.add_all(data)
            self.session.flush()
            for datum in data:
                self.session.expunge(datum)
            return data

    def delete(self, item_id: Any) -> ModelT:
        """Delete instance identified by ``item_id``.

        Args:
            item_id: Identifier of instance to be deleted.

        Returns:
            The deleted instance.

        Raises:
            NotFoundError: If no instance found identified by ``item_id``.
        """
        with wrap_sqlalchemy_exception():
            instance = self.get(item_id)
            self.session.delete(instance)
            self.session.flush()
            self.session.expunge(instance)
            return instance

    def delete_many(self, item_ids: list[Any]) -> list[ModelT]:
        """Delete instance identified by `item_id`.

        Args:
            item_ids: Identifier of instance to be deleted.

        Returns:
            The deleted instances.

        """
        with wrap_sqlalchemy_exception():
            instances: list[ModelT] = []
            chunk_size = 450
            for idx in range(0, len(item_ids), chunk_size):
                chunk = item_ids[idx : min(idx + chunk_size, len(item_ids))]
                if self._dialect.delete_executemany_returning:
                    instances.extend(
                        self.session.scalars(
                            delete(self.model_type)
                            .where(getattr(self.model_type, self.id_attribute).in_(chunk))
                            .returning(self.model_type)
                        )
                    )
                else:
                    instances.extend(
                        self.session.scalars(
                            select(self.model_type).where(getattr(self.model_type, self.id_attribute).in_(chunk))
                        )
                    )
                    self.session.execute(
                        delete(self.model_type).where(getattr(self.model_type, self.id_attribute).in_(chunk))
                    )
            self.session.flush()
            for instance in instances:
                self.session.expunge(instance)
            return instances

    def exists(self, **kwargs: Any) -> bool:
        """Return true if the object specified by ``kwargs`` exists.

        Args:
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            True if the instance was found.  False if not found..

        """
        existing = self.count(**kwargs)
        return existing > 0

    def get(self, item_id: Any, **kwargs: Any) -> ModelT:
        """Get instance identified by `item_id`.

        Args:
            item_id: Identifier of the instance to be retrieved.
            **kwargs: Additional parameters

        Returns:
            The retrieved instance.

        Raises:
            NotFoundError: If no instance found identified by `item_id`.
        """
        with wrap_sqlalchemy_exception():
            statement = kwargs.pop("statement", self.statement)
            statement = self._filter_select_by_kwargs(statement=statement, **{self.id_attribute: item_id})
            instance = (self._execute(statement)).scalar_one_or_none()
            instance = self.check_not_found(instance)
            self.session.expunge(instance)
            return instance

    def get_one(self, **kwargs: Any) -> ModelT:
        """Get instance identified by ``kwargs``.

        Args:
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            The retrieved instance.

        Raises:
            NotFoundError: If no instance found identified by `item_id`.
        """
        with wrap_sqlalchemy_exception():
            statement = kwargs.pop("statement", self.statement)
            statement = self._filter_select_by_kwargs(statement=statement, **kwargs)
            instance = (self._execute(statement)).scalar_one_or_none()
            instance = self.check_not_found(instance)
            self.session.expunge(instance)
            return instance

    def get_one_or_none(self, **kwargs: Any) -> ModelT | None:
        """Get instance identified by ``kwargs`` or None if not found.

        Args:
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            The retrieved instance or None
        """
        with wrap_sqlalchemy_exception():
            statement = kwargs.pop("statement", self.statement)
            statement = self._filter_select_by_kwargs(statement=statement, **kwargs)
            instance = (self._execute(statement)).scalar_one_or_none()
            if instance:
                self.session.expunge(instance)
            return instance  # type: ignore

    def get_or_create(
        self, match_fields: list[str] | str | None = None, upsert: bool = True, **kwargs: Any
    ) -> tuple[ModelT, bool]:
        """Get instance identified by ``kwargs`` or create if it doesn't exist.

        Args:
            match_fields: a list of keys to use to match the existing model.  When empty, all fields are matched.
            upsert: When using match_fields and actual model values differ from `kwargs`, perform an update operation on the model.
            **kwargs: Identifier of the instance to be retrieved.

        Returns:
            a tuple that includes the instance and whether or not it needed to be created.  When using match_fields and actual model values differ from `kwargs`, the model value will be updated.
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
            self.session.flush()
            self.session.refresh(existing)
            self.session.expunge(existing)
        return existing, False

    def count(self, *filters: FilterTypes, **kwargs: Any) -> int:
        """Get the count of records returned by a query.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query, ignoring pagination.
        """
        statement = kwargs.pop("statement", self.statement)
        statement = statement.with_only_columns(
            sql_func.count(self.get_id_attribute_value(self.model_type)),
            maintain_column_froms=True,
        ).order_by(None)
        statement = self._apply_filters(*filters, apply_pagination=False, statement=statement)
        statement = self._filter_select_by_kwargs(statement, **kwargs)
        results = self._execute(statement)
        return results.scalar_one()  # type: ignore

    def update(self, data: ModelT) -> ModelT:
        """Update instance with the attribute values present on `data`.

        Args:
            data: An instance that should have a value for `self.id_attribute` that exists in the
                collection.

        Returns:
            The updated instance.

        Raises:
            NotFoundError: If no instance found with same identifier as `data`.
        """
        with wrap_sqlalchemy_exception():
            item_id = self.get_id_attribute_value(data)
            # this will raise for not found, and will put the item in the session
            self.get(item_id)
            # this will merge the inbound data to the instance we just put in the session
            instance = self._attach_to_session(data, strategy="merge")
            self.session.flush()
            self.session.refresh(instance)
            self.session.expunge(instance)
            return instance

    def update_many(self, data: list[ModelT]) -> list[ModelT]:
        """Update one or more instances with the attribute values present on `data`.

        This function has an optimized bulk insert based on the configured SQL dialect:
        - For backends supporting `RETURNING` with `executemany`, a single bulk insert with returning clause is executed.
        - For other backends, it does a bulk insert and then selects the inserted records

        Args:
            data: A list of instances to update.  Each should have a value for `self.id_attribute` that exists in the
                collection.

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
                self.session.flush()
                for instance in instances:
                    self.session.expunge(instance)
                return instances
            self.session.execute(
                update(self.model_type),
                data_to_update,
            )
            self.session.flush()
            return data

    def list_and_count(
        self,
        *filters: FilterTypes,
        **kwargs: Any,
    ) -> tuple[list[ModelT], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query, ignoring pagination.
        """
        if self._dialect.name in {"spanner", "spanner+spanner"}:
            return self._list_and_count_basic(*filters, **kwargs)
        return self._list_and_count_window(*filters, **kwargs)

    def _list_and_count_window(
        self,
        *filters: FilterTypes,
        **kwargs: Any,
    ) -> tuple[list[ModelT], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query using an analytical window function, ignoring pagination.
        """
        statement = kwargs.pop("statement", self.statement)
        statement = statement.add_columns(over(sql_func.count(self.get_id_attribute_value(self.model_type))))
        statement = self._apply_filters(*filters, statement=statement)
        statement = self._filter_select_by_kwargs(statement, **kwargs)
        with wrap_sqlalchemy_exception():
            result = self._execute(statement)
            count: int = 0
            instances: list[ModelT] = []
            for i, (instance, count_value) in enumerate(result):
                self.session.expunge(instance)
                instances.append(instance)
                if i == 0:
                    count = count_value
            return instances, count

    def _list_and_count_basic(
        self,
        *filters: FilterTypes,
        **kwargs: Any,
    ) -> tuple[list[ModelT], int]:
        """List records with total count.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            Count of records returned by query using 2 queries, ignoring pagination.
        """
        statement = kwargs.pop("statement", self.statement)
        statement = self._apply_filters(*filters, statement=statement)
        statement = self._filter_select_by_kwargs(statement, **kwargs)
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
                self.session.expunge(instance)
                instances.append(instance)
            return instances, count

    def list(self, *filters: FilterTypes, **kwargs: Any) -> list[ModelT]:
        """Get a list of instances, optionally filtered.

        Args:
            *filters: Types for specific filtering operations.
            **kwargs: Instance attribute value filters.

        Returns:
            The list of instances, after filtering applied.
        """
        statement = kwargs.pop("statement", self.statement)
        statement = self._apply_filters(*filters, statement=statement)
        statement = self._filter_select_by_kwargs(statement, **kwargs)

        with wrap_sqlalchemy_exception():
            result = self._execute(statement)
            instances = list(result.scalars())
            for instance in instances:
                self.session.expunge(instance)
            return instances

    def upsert(self, data: ModelT) -> ModelT:
        """Update or create instance.

        Updates instance with the attribute values present on `data`, or creates a new instance if
        one doesn't exist.

        Args:
            data: Instance to update existing, or be created. Identifier used to determine if an
                existing instance exists is the value of an attribute on `data` named as value of
                `self.id_attribute`.

        Returns:
            The updated or created instance.

        Raises:
            NotFoundError: If no instance found with same identifier as `data`.
        """
        with wrap_sqlalchemy_exception():
            instance = self._attach_to_session(data, strategy="merge")
            self.session.flush()
            self.session.refresh(instance)
            self.session.expunge(instance)
            return instance

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
            `True` if healthy.
        """
        return (  # type:ignore[no-any-return]  # pragma: no cover
            session.execute(text("SELECT 1"))
        ).scalar_one() == 1

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
                    filter_.field_name, filter_.before, filter_.after, statement=statement
                )
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
            else:
                raise RepositoryError(f"Unexpected filter: {filter_}")
        return statement

    def _filter_in_collection(self, field_name: str, values: abc.Collection[Any], statement: SelectT) -> SelectT:
        if not values:
            return statement
        return statement.where(getattr(self.model_type, field_name).in_(values))

    def _filter_on_datetime_field(
        self, field_name: str, before: datetime | None, after: datetime | None, statement: SelectT
    ) -> SelectT:
        field = getattr(self.model_type, field_name)
        if before is not None:
            statement = statement.where(field < before)
        if after is not None:
            statement = statement.where(field > after)
        return statement

    def _filter_select_by_kwargs(self, statement: SelectT, **kwargs: Any) -> SelectT:
        for key, val in kwargs.items():
            statement = statement.where(getattr(self.model_type, key) == val)
        return statement

    def _filter_by_like(self, statement: SelectT, field_name: str, value: str, ignore_case: bool) -> SelectT:
        field = getattr(self.model_type, field_name)
        search_text = f"%{value}%"
        return statement.where(field.ilike(search_text) if ignore_case else field.like(search_text))

    def _order_by(self, statement: SelectT, field_name: str, sort_desc: bool = False) -> SelectT:
        field = getattr(self.model_type, field_name)
        return statement.order_by(field.desc() if sort_desc else field.asc())
