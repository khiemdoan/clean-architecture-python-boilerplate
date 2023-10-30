
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'
__url__ = 'https://github.com/khiemdoan/clean-architecture-python-boilerplate/blob/main/src/models/base.py'

# Original source from `advanced-alchemy` project
# https://github.com/jolt-org/advanced-alchemy/blob/main/advanced_alchemy/base.py
# Modified by: Khiem Doan

"""Application ORM configuration."""

import contextlib
import re
from datetime import date, datetime, timezone
from typing import Any, ClassVar, Protocol, TypeVar, runtime_checkable
from uuid import UUID, uuid4

from advanced_alchemy.types import GUID, BigIntIdentity, DateTimeUTC, JsonB
from sqlalchemy import Date, MetaData, Sequence, String, func
from sqlalchemy.event import listens_for
from sqlalchemy.orm import (DeclarativeBase, Mapped, Mapper, Session, declared_attr, mapped_column, orm_insert_sentinel,
                            registry)
from sqlalchemy.sql import FromClause
from sqlalchemy.sql.schema import _NamingSchemaParameter as NamingSchemaParameter
from sqlalchemy.types import TypeEngine

__all__ = (
    "AssociationAuditBase",
    "AssociationBase",
    "AuditColumns",
    "BigIntAuditBase",
    "BigIntBase",
    "BigIntPrimaryKey",
    "CommonTableAttributes",
    "create_registry",
    "ModelProtocol",
    "touch_updated_timestamp",
    "UUIDAuditBase",
    "UUIDBase",
    "UUIDPrimaryKey",
)


UUIDBaseT = TypeVar("UUIDBaseT", bound="UUIDBase")
BigIntBaseT = TypeVar("BigIntBaseT", bound="BigIntBase")


def same_as(column_name):
    def default_function(context):
        return context.current_parameters.get(column_name)
    return default_function


convention: NamingSchemaParameter = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
"""Templates for automated constraint name generation."""


@listens_for(Session, "before_flush")
def touch_updated_timestamp(session: Session, *_: Any) -> None:
    """Set timestamp on update.

    Called from SQLAlchemy's
    :meth:`before_flush <sqlalchemy.orm.SessionEvents.before_flush>` event to bump the ``updated``
    timestamp on modified instances.

    Args:
        session: The sync :class:`Session <sqlalchemy.orm.Session>` instance that underlies the async
            session.
    """
    for instance in session.dirty:
        if hasattr(instance, "updated_at"):
            instance.updated_at = datetime.now(timezone.utc)


@runtime_checkable
class ModelProtocol(Protocol):
    """The base SQLAlchemy model protocol."""

    __table__: FromClause
    __mapper__: Mapper
    __name__: ClassVar[str]

    def to_dict(self, exclude: set[str] | None = None) -> dict[str, Any]:
        """Convert model to dictionary.

        Returns:
            dict[str, Any]: A dict representation of the model
        """
        ...


class UUIDPrimaryKey:
    """UUID Primary Key Field Mixin."""

    id: Mapped[UUID] = mapped_column(server_default=func.gen_random_uuid(), default=uuid4, primary_key=True)
    """UUID Primary key column."""

    # noinspection PyMethodParameters
    @declared_attr
    def _sentinel(cls) -> Mapped[int]:
        return orm_insert_sentinel(name="sa_orm_sentinel")


class BigIntPrimaryKey:
    """BigInt Primary Key Field Mixin."""

    # noinspection PyMethodParameters
    @declared_attr
    def id(cls) -> Mapped[int]:
        """BigInt Primary key column."""
        return mapped_column(
            BigIntIdentity,
            Sequence(f"{cls.__tablename__}_id_seq", optional=False),  # type: ignore[attr-defined]
            primary_key=True,
        )


class AuditColumns:
    """Created/Updated At Fields Mixin."""

    created_at: Mapped[datetime] = mapped_column(
        DateTimeUTC(timezone=True),
        server_default=func.now(),
        default=lambda: datetime.now(timezone.utc),
    )
    """Date/time of instance creation."""
    updated_at: Mapped[datetime] = mapped_column(
        DateTimeUTC(timezone=True),
        server_default=func.now(),
        default=lambda: datetime.now(timezone.utc),
    )
    """Date/time of instance last update."""


def create_registry() -> registry:
    """Create a new SQLAlchemy registry."""
    meta = MetaData(naming_convention=convention)
    type_annotation_map: dict[type, type[TypeEngine[Any]] | TypeEngine[Any]] = {
        UUID: GUID,
        datetime: DateTimeUTC,
        date: Date,
        dict: JsonB,
    }
    with contextlib.suppress(ImportError):
        from pydantic import AnyHttpUrl, AnyUrl, EmailStr

        type_annotation_map.update({EmailStr: String, AnyUrl: String, AnyHttpUrl: String})
    return registry(metadata=meta, type_annotation_map=type_annotation_map)


orm_registry = create_registry()


class Model(DeclarativeBase):
    """Common attributes for SQLALchemy tables."""

    __name__: ClassVar[str]
    __table__: FromClause
    __mapper__: Mapper

    registry = orm_registry

    # noinspection PyMethodParameters
    @declared_attr.directive
    def __tablename__(cls) -> str:
        """Infer table name from class name."""
        regexp = re.compile("((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))")
        return regexp.sub(r"_\1", cls.__name__).lower()

    def to_dict(self, exclude: set[str] | None = None) -> dict[str, Any]:
        """Convert model to dictionary.

        Returns:
            dict[str, Any]: A dict representation of the model
        """
        exclude = {"sa_orm_sentinel", "_sentinel"}.union(self._sa_instance_state.unloaded).union(exclude or [])  # type: ignore[attr-defined]
        return {field.name: getattr(self, field.name) for field in self.__table__.columns if field.name not in exclude}


class UUIDBase(Model, UUIDPrimaryKey):
    """Base for all SQLAlchemy declarative models with UUID primary keys."""

    __abstract__ = True


class UUIDAuditBase(Model, UUIDPrimaryKey, AuditColumns):
    """Base for declarative models with UUID primary keys and audit columns."""

    __abstract__ = True


class BigIntBase(Model, BigIntPrimaryKey):
    """Base for all SQLAlchemy declarative models with BigInt primary keys."""

    __abstract__ = True


class BigIntAuditBase(Model, BigIntPrimaryKey, AuditColumns):
    """Base for declarative models with BigInt primary keys and audit columns."""

    __abstract__ = True


class AssociationBase(Model):
    """Base for declarative models without primary keys."""

    __abstract__ = True


class AssociationAuditBase(Model, AuditColumns):
    """Base for declarative models with audit columns but without primary keys."""

    __abstract__ = True
