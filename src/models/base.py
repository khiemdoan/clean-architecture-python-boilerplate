__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'
__url__ = 'https://github.com/khiemdoan/clean-architecture-python-boilerplate/blob/main/src/models/base.py'

# Original source from `advanced-alchemy` project
# https://github.com/litestar-org/advanced-alchemy/blob/main/advanced_alchemy/base.py
# Modified by: Khiem Doan

"""Application ORM configuration."""

import contextlib
import re
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable
from uuid import UUID

from advanced_alchemy.types import GUID, NANOID_INSTALLED, UUID_UTILS_INSTALLED, BigIntIdentity, DateTimeUTC, JsonB
from sqlalchemy import Date, Index, MetaData, Sequence, String, UniqueConstraint
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Mapper,
    declarative_mixin,
    declared_attr,
    mapped_column,
    orm_insert_sentinel,
    registry,
    validates,
)
from sqlalchemy.orm.decl_base import _TableArgsType as TableArgsType  # pyright: ignore[reportPrivateUsage]
from typing_extensions import TypeVar

if UUID_UTILS_INSTALLED and not TYPE_CHECKING:
    from uuid_utils.compat import uuid4, uuid6, uuid7  # pyright: ignore[reportMissingImports]

else:
    from uuid import uuid4  # type: ignore[assignment]

    uuid6 = uuid4  # type: ignore[assignment]
    uuid7 = uuid4  # type: ignore[assignment]

if NANOID_INSTALLED and not TYPE_CHECKING:
    from fastnanoid import generate as nanoid  # pyright: ignore[reportMissingImports]

else:
    nanoid = uuid4  # type: ignore[assignment]

if TYPE_CHECKING:
    from sqlalchemy.sql import FromClause
    from sqlalchemy.sql.schema import _NamingSchemaParameter as NamingSchemaParameter  # pyright: ignore[reportPrivateUsage]
    from sqlalchemy.types import TypeEngine


__all__ = (
    'AuditColumns',
    'BigIntAuditBase',
    'BigIntBase',
    'BigIntPrimaryKey',
    'CommonTableAttributes',
    'create_registry',
    'ModelProtocol',
    'UUIDAuditBase',
    'UUIDBase',
    'UUIDv6AuditBase',
    'UUIDv6Base',
    'UUIDv7AuditBase',
    'UUIDv7Base',
    'NanoIDAuditBase',
    'NanoIDBase',
    'NanoIDPrimaryKey',
    'UUIDPrimaryKey',
    'UUIDv7PrimaryKey',
    'UUIDv6PrimaryKey',
    'SlugKey',
    'SQLQuery',
    'orm_registry',
    'merge_table_arguments',
    'TableArgsType',
    'BasicAttributes',
)


UUIDBaseT = TypeVar('UUIDBaseT', bound='UUIDBase')
BigIntBaseT = TypeVar('BigIntBaseT', bound='BigIntBase')
UUIDv6BaseT = TypeVar('UUIDv6BaseT', bound='UUIDv6Base')
UUIDv7BaseT = TypeVar('UUIDv7BaseT', bound='UUIDv7Base')
NanoIDBaseT = TypeVar('NanoIDBaseT', bound='NanoIDBase')


def same_as(column_name):
    def default_function(context):
        return context.current_parameters.get(column_name)

    return default_function


convention: NamingSchemaParameter = {
    'ix': 'ix_%(column_0_label)s',
    'uq': 'uq_%(table_name)s_%(column_0_name)s',
    'ck': 'ck_%(table_name)s_%(constraint_name)s',
    'fk': 'fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s',
    'pk': 'pk_%(table_name)s',
}
"""Templates for automated constraint name generation."""
table_name_regexp = re.compile('((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))')
"""Regular expression for table name"""


def merge_table_arguments(cls: type[DeclarativeBase], table_args: TableArgsType | None = None) -> TableArgsType:
    """Merge Table Arguments.

    When using mixins that include their own table args, it is difficult to append info into the model such as a comment.

    This function helps you merge the args together.

    Args:
        cls (DeclarativeBase): This is the model that will get the table args
        table_args: additional information to add to table_args

    Returns:
        tuple | dict: The merged __table_args__ property
    """
    args: list[Any] = []
    kwargs: dict[str, Any] = {}

    mixin_table_args = (getattr(super(base_cls, cls), '__table_args__', None) for base_cls in cls.__bases__)  # pyright: ignore[reportUnknownParameter,reportUnknownArgumentType,reportArgumentType]

    for arg_to_merge in (*mixin_table_args, table_args):
        if arg_to_merge:
            if isinstance(arg_to_merge, tuple):
                last_positional_arg = arg_to_merge[-1]  # pyright: ignore[reportUnknownVariableType]
                args.extend(arg_to_merge[:-1])  # pyright: ignore[reportUnknownArgumentType]
                if isinstance(last_positional_arg, dict):
                    kwargs.update(last_positional_arg)  # pyright: ignore[reportUnknownArgumentType]
                else:
                    args.append(last_positional_arg)
            else:
                kwargs.update(arg_to_merge)

    if args:
        if kwargs:
            return (*args, kwargs)
        return tuple(args)
    return kwargs


@runtime_checkable
class ModelProtocol(Protocol):
    """The base SQLAlchemy model protocol."""

    if TYPE_CHECKING:
        __table__: FromClause
        __mapper__: Mapper[Any]
        __name__: str

    def to_dict(self, exclude: set[str] | None = None) -> dict[str, Any]:
        """Convert model to dictionary.

        Returns:
            Dict[str, Any]: A dict representation of the model
        """
        ...


class UUIDPrimaryKey:
    """UUID Primary Key Field Mixin."""

    id: Mapped[UUID] = mapped_column(default=uuid4, primary_key=True)
    """UUID Primary key column."""

    @declared_attr
    def _sentinel(cls) -> Mapped[int]:
        return orm_insert_sentinel(name='sa_orm_sentinel')


class UUIDv6PrimaryKey:
    """UUID v6 Primary Key Field Mixin."""

    id: Mapped[UUID] = mapped_column(default=uuid6, primary_key=True)
    """UUID Primary key column."""

    @declared_attr
    def _sentinel(cls) -> Mapped[int]:
        return orm_insert_sentinel(name='sa_orm_sentinel')


class UUIDv7PrimaryKey:
    """UUID v7 Primary Key Field Mixin."""

    id: Mapped[UUID] = mapped_column(default=uuid7, primary_key=True)
    """UUID Primary key column."""

    @declared_attr
    def _sentinel(cls) -> Mapped[int]:
        return orm_insert_sentinel(name='sa_orm_sentinel')


class NanoIDPrimaryKey:
    """Nano ID Primary Key Field Mixin."""

    id: Mapped[str] = mapped_column(default=nanoid, primary_key=True)
    """Nano ID Primary key column."""

    @declared_attr
    def _sentinel(cls) -> Mapped[int]:
        return orm_insert_sentinel(name='sa_orm_sentinel')


class BigIntPrimaryKey:
    """BigInt Primary Key Field Mixin."""

    # noinspection PyMethodParameters
    @declared_attr
    def id(cls) -> Mapped[int]:
        """BigInt Primary key column."""
        return mapped_column(
            BigIntIdentity,
            Sequence(f'{cls.__tablename__}_id_seq', optional=False),  # type: ignore[attr-defined]
            primary_key=True,
        )


class AuditColumns:
    """Created/Updated At Fields Mixin."""

    created_at: Mapped[datetime] = mapped_column(
        DateTimeUTC(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    """Date/time of instance creation."""
    updated_at: Mapped[datetime] = mapped_column(
        DateTimeUTC(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    """Date/time of instance last update."""

    @validates('created_at', 'updated_at')
    def validate_tz_info(self, _: str, value: datetime) -> datetime:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value


def create_registry(
    custom_annotation_map: dict[Any, type[TypeEngine[Any]] | TypeEngine[Any]] | None = None,
) -> registry:
    """Create a new SQLAlchemy registry."""
    import uuid as core_uuid

    meta = MetaData(naming_convention=convention)
    type_annotation_map: dict[Any, type[TypeEngine[Any]] | TypeEngine[Any]] = {
        UUID: GUID,
        core_uuid.UUID: GUID,
        datetime: DateTimeUTC,
        date: Date,
        dict: JsonB,
    }
    with contextlib.suppress(ImportError):
        from pydantic import AnyHttpUrl, AnyUrl, EmailStr, Json

        type_annotation_map.update({EmailStr: String, AnyUrl: String, AnyHttpUrl: String, Json: JsonB})
    with contextlib.suppress(ImportError):
        from msgspec import Struct

        type_annotation_map[Struct] = JsonB
    if custom_annotation_map is not None:
        type_annotation_map.update(custom_annotation_map)
    return registry(metadata=meta, type_annotation_map=type_annotation_map)


orm_registry = create_registry()


class Model(DeclarativeBase):
    """Basic attributes for SQLALchemy tables and queries."""

    if TYPE_CHECKING:
        __name__: str
        __table__: FromClause
        __mapper__: Mapper[Any]

    if TYPE_CHECKING:
        __tablename__: str
    else:

        @declared_attr.directive
        def __tablename__(cls) -> str:
            """Infer table name from class name."""

            return table_name_regexp.sub(r'_\1', cls.__name__).lower()

    def to_dict(self, exclude: set[str] | None = None) -> dict[str, Any]:
        """Convert model to dictionary.

        Returns:
            Dict[str, Any]: A dict representation of the model
        """
        exclude = {'sa_orm_sentinel', '_sentinel'}.union(self._sa_instance_state.unloaded).union(exclude or [])  # type: ignore[attr-defined]
        return {
            field: getattr(self, field)
            for field in self.__mapper__.columns.keys()  # noqa: SIM118
            if field not in exclude
        }


@declarative_mixin
class SlugKey:
    """Slug unique Field Model Mixin."""

    @declared_attr
    def slug(cls) -> Mapped[str]:
        """Slug field."""
        return mapped_column(
            String(length=100),
            nullable=False,
        )

    @staticmethod
    def _create_unique_slug_index(*_args: Any, **kwargs: Any) -> bool:
        return bool(kwargs['dialect'].name.startswith('spanner'))

    @staticmethod
    def _create_unique_slug_constraint(*_args: Any, **kwargs: Any) -> bool:
        return not kwargs['dialect'].name.startswith('spanner')

    @declared_attr.directive
    @classmethod
    def __table_args__(cls) -> TableArgsType:
        return (
            UniqueConstraint(
                cls.slug,
                name=f'uq_{cls.__tablename__}_slug',  # type: ignore[attr-defined]
            ).ddl_if(callable_=cls._create_unique_slug_constraint),
            Index(
                f'ix_{cls.__tablename__}_slug_unique',  # type: ignore[attr-defined]
                cls.slug,
                unique=True,
            ).ddl_if(callable_=cls._create_unique_slug_index),
        )


class UUIDBase(Model, UUIDPrimaryKey):
    """Base for all SQLAlchemy declarative models with UUID primary keys."""

    __abstract__ = True


class UUIDAuditBase(Model, UUIDPrimaryKey, AuditColumns):
    """Base for declarative models with UUID primary keys and audit columns."""

    __abstract__ = True


class UUIDv6Base(Model, UUIDv6PrimaryKey):
    """Base for all SQLAlchemy declarative models with UUID primary keys."""

    __abstract__ = True


class UUIDv6AuditBase(Model, UUIDv6PrimaryKey, AuditColumns):
    """Base for declarative models with UUID primary keys and audit columns."""

    __abstract__ = True


class UUIDv7Base(Model, UUIDv7PrimaryKey):
    """Base for all SQLAlchemy declarative models with UUID primary keys."""

    __abstract__ = True


class UUIDv7AuditBase(Model, UUIDv7PrimaryKey, AuditColumns):
    """Base for declarative models with UUID primary keys and audit columns."""

    __abstract__ = True


class NanoIDBase(Model, NanoIDPrimaryKey):
    """Base for all SQLAlchemy declarative models with Nano ID primary keys."""

    __abstract__ = True


class NanoIDAuditBase(Model, NanoIDPrimaryKey, AuditColumns):
    """Base for declarative models with Nano ID primary keys and audit columns."""

    __abstract__ = True


class BigIntBase(Model, BigIntPrimaryKey):
    """Base for all SQLAlchemy declarative models with BigInt primary keys."""

    __abstract__ = True


class BigIntAuditBase(Model, BigIntPrimaryKey, AuditColumns):
    """Base for declarative models with BigInt primary keys and audit columns."""

    __abstract__ = True


class SQLQuery(Model):
    """Base for all SQLAlchemy custom mapped objects."""

    __abstract__ = True
    __allow_unmapped__ = True


class AssociationBase(Model):
    """Base for declarative models without primary keys."""

    __abstract__ = True


class AssociationAuditBase(Model, AuditColumns):
    """Base for declarative models with audit columns but without primary keys."""

    __abstract__ = True
