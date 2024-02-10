import contextlib
import dataclasses
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Generic, TypeAlias, TypeVar

import aiosqlite
from pypika import AliasedQuery
from pypika.queries import (
    CreateIndexBuilder,
    CreateQueryBuilder,
    DropQueryBuilder,
    QueryBuilder,
)

T = TypeVar("T", bound="DBWrapper")
T_QUERY = TypeVar("T_QUERY")

ParametersType: TypeAlias = dict[str, Any]


@dataclasses.dataclass
class _QueryWithParameters(Generic[T_QUERY]):
    query: T_QUERY
    parameters: ParametersType = dataclasses.field(default_factory=dict)


CreateQueryWithParameters = _QueryWithParameters[
    CreateQueryBuilder | CreateIndexBuilder
]
QueryWithParameters = _QueryWithParameters[QueryBuilder]
DropQueryWithParameters = _QueryWithParameters[DropQueryBuilder]
AliasedQueryWithParameters = _QueryWithParameters[AliasedQuery]


@dataclasses.dataclass
class DBWrapper(object):
    connection: aiosqlite.Connection

    @classmethod
    async def create(
        cls: type[T],
        database: str | Path,
        journal_mode: str = "WAL",
        enforce_foreign_keys: bool = True,
        row_factory: type[aiosqlite.Row] = aiosqlite.Row,
        initialize: bool = True,
    ) -> T:
        connection = await aiosqlite.connect(database=database)
        connection.row_factory = row_factory

        if initialize:
            await connection.execute(f"PRAGMA journal_mode={journal_mode};")
            await connection.execute(
                f"PRAGMA foreign_keys={'ON' if enforce_foreign_keys else 'OFF'}"
            )

        return cls(connection=connection)

    async def __aenter__(self) -> "DBWrapper":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    @contextlib.asynccontextmanager
    async def reader(self) -> AsyncIterator[aiosqlite.Connection]:
        yield self.connection

    @contextlib.asynccontextmanager
    async def writer(self) -> AsyncIterator[aiosqlite.Connection]:
        if self.connection.in_transaction:
            yield self.connection
            return

        try:
            yield self.connection

        except Exception:
            await self.connection.rollback()
            raise

        else:
            await self.connection.commit()

    async def close(self) -> None:
        if self.connection._connection is None:
            return

        await self.connection.close()
