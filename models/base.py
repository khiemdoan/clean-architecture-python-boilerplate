
__author__ = 'Khiem Doan'
__github__ = 'https://github.com/khiemdoan'
__email__ = 'doankhiem.crazy@gmail.com'

# Original source from `litestar` project
# https://github.com/litestar-org/litestar/blob/main/litestar/contrib/sqlalchemy/base.py
# Modified by: Khiem Doan

"""Application ORM configuration."""

import re
from datetime import date, datetime, timezone
from typing import Any, ClassVar, Protocol, runtime_checkable
from uuid import UUID, uuid4

from pydantic import AnyHttpUrl, AnyUrl, EmailStr
from sqlalchemy import BigInteger, Date, DateTime, MetaData, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, declared_attr, mapped_column, registry
from sqlalchemy.sql import FromClause

same_as = lambda col: lambda ctx: ctx.current_parameters.get(col)


convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
"""Templates for automated constraint name generation."""


@runtime_checkable
class ModelProtocol(Protocol):
    """The base SQLAlchemy model protocol."""

    __table__: FromClause
    __name__: ClassVar[str]

    def to_dict(self, exclude: set[str] | None = None) -> dict[str, Any]:
        """Convert model to dictionary.

        Returns:
            dict[str, Any]: A dict representation of the model
        """
        ...


class UUIDPrimaryKey:
    """UUID Primary Key Field Mixin."""

    id: Mapped[UUID] = mapped_column(server_default=func.gen_random_uuid(), default=uuid4, primary_key=True)  # pyright: ignore
    """UUID Primary key column."""


class BigIntPrimaryKey:
    """BigInt Primary Key Field Mixin."""

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

class AuditColumns:
    """Created/Updated At Fields Mixin."""

    created_at: Mapped[datetime] = mapped_column(  # pyright: ignore
        DateTime(timezone=True),
        server_default=func.now(),
        default=lambda: datetime.now(timezone.utc),
    )
    """Date/time of instance creation."""
    updated_at: Mapped[datetime] = mapped_column(  # pyright: ignore
        DateTime(timezone=True),
        server_default=func.now(),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    """Date/time of instance last update."""


def create_registry() -> registry:
    """Create a new SQLAlchemy registry."""
    meta = MetaData(naming_convention=convention)
    return registry(
        metadata=meta,
        type_annotation_map={
            EmailStr: String,
            AnyUrl: String,
            AnyHttpUrl: String,
            date: Date,
        },
    )


orm_registry = create_registry()


class Model(DeclarativeBase):
    """Common attributes for SQLALchemy tables."""

    __name__: ClassVar[str]
    __table__: FromClause
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
