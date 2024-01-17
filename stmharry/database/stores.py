# NOTE(stmharry): this is an experiment to see if we can master SQL rawdog style
import dataclasses
import json
import sqlite3
from collections.abc import Iterable
from typing import Any, ClassVar, Generic, Literal, TypeVar, get_args

import aiosqlite
from absl import logging
from pypika import AliasedQuery, Field, Parameter, Table
from pypika.queries import QueryBuilder, Selectable
from pypika.terms import Term

from stmharry.database.core import (
    CreateQueryWithParameters,
    DBWrapper,
    ParametersType,
    QueryWithParameters,
)
from stmharry.database.pypika_dialects import SQLQueryBuilder
from stmharry.database.schemas import BaseRow
from stmharry.rust import Err, Ok, Result

T = TypeVar("T", bound="BaseStore")
T_ROW = TypeVar("T_ROW", bound=BaseRow)
T_ROW_RETURN = TypeVar("T_ROW_RETURN", bound=BaseRow)


@dataclasses.dataclass(kw_only=True)
class BaseStore(Generic[T_ROW]):
    ID_COLUMN: ClassVar[str] = "id"
    TABLE_NAME: ClassVar[str]
    CREATE_TABLE_SQLS: ClassVar[list[str]]
    ON_CONFLICT: ClassVar[Literal["do_nothing", "do_update"]] = "do_nothing"

    db: DBWrapper
    _row_cls: type[T_ROW] = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self._row_cls = get_args(self.__class__.__orig_bases__[0])[0]  # type: ignore

    @classmethod
    async def create(cls: type[T], db: DBWrapper, *args: Any, **kwargs: Any) -> T:
        self: T = cls(db=db, *args, **kwargs)

        async with db.writer() as connection:
            qp: CreateQueryWithParameters
            for qp in self._create_table_queries():
                try:
                    await connection.execute(
                        sql=qp.query.get_sql(), parameters=qp.parameters
                    )

                except sqlite3.OperationalError as e:
                    sql: str = qp.query.get_sql()
                    logging.info(f"Failed to execute '{sql}': {e}")

                    raise

        return self

    @classmethod
    def _return_one(
        cls, rows: Iterable[aiosqlite.Row], row_cls: type[T_ROW_RETURN]
    ) -> Result[T_ROW_RETURN, ValueError]:
        for row in rows:
            result: Result = row_cls.model_validate_row(row)

            match result:
                case Ok(_row):
                    return Ok(_row)

                case Err(_):
                    continue

        else:
            return Err(ValueError(f"Failed to find {row_cls} in {cls.TABLE_NAME}"))

    @classmethod
    def _return_one_or_none(
        cls, rows: Iterable[aiosqlite.Row], row_cls: type[T_ROW_RETURN]
    ) -> T_ROW_RETURN | None:
        for row in rows:
            result: Result = row_cls.model_validate_row(row)

            match result:
                case Ok(_row):
                    return _row

                case Err(_):
                    continue

        else:
            return None

    @classmethod
    def _return_many(
        cls, rows: Iterable[aiosqlite.Row], row_cls: type[T_ROW_RETURN]
    ) -> list[T_ROW_RETURN]:
        _rows: list[T_ROW_RETURN] = []
        for row in rows:
            result: Result = row_cls.model_validate_row(row)

            match result:
                case Ok(_row):
                    _rows.append(_row)

                case Err(_):
                    continue

        return _rows

    @property
    def column_names(self) -> list[str]:
        return list(self._row_cls.model_fields.keys())

    @property
    def columns(self) -> list[Field]:
        return [Field(column_name) for column_name in self.column_names]

    # query builders

    # NOTE: pypika does not support full `cte-table-name` specification
    @classmethod
    def _make_aliased_query(
        cls, query: Selectable, name: str, column_names: list[str]
    ) -> AliasedQuery:
        alias: str = "{table_name} ({column_names})".format(
            table_name=name,
            column_names=",".join(
                '"{}"'.format(column_name) for column_name in column_names
            ),
        )
        return AliasedQuery(alias, query=query)

    @classmethod
    def _make_table_from_aliased_query(cls, query: AliasedQuery) -> Table:
        alias: str = query.alias
        table_name: str = alias.split("(")[0].strip()
        return Table(table_name)

    @classmethod
    def _query_builder(cls) -> SQLQueryBuilder:
        return SQLQueryBuilder(
            wrap_set_operation_queries=False,
        )

    def _create_table_queries(
        self,
    ) -> list[CreateQueryWithParameters]:
        return []

    def _create_row_query(
        self,
        rows: list[T_ROW],
        *,
        table: Table | None = None,
        columns: list[Field] | None = None,
    ) -> QueryWithParameters:
        table = table or Table(self.TABLE_NAME)
        columns = columns or self.columns

        query: QueryBuilder = self._query_builder().into(table).columns(*columns)
        parameters: ParametersType = {}
        for num, row in enumerate(rows):
            row_dict: dict[str, Any] = json.loads(row.model_dump_json())

            values: list[Any] = []
            column: Field
            for column in columns:
                key: str = f"{column.name}_{num}"

                values.append(Parameter(f":{key}"))
                parameters[key] = row_dict[column.name]

            query = query.insert(*values)

        query = query.on_conflict(self.ID_COLUMN)
        match self.ON_CONFLICT:
            case "do_nothing":
                query = query.do_nothing()

            case "do_update":
                for column in columns:
                    if column.name == self.ID_COLUMN:
                        continue

                    query = query.do_update(update_field=column)

        query = query.returning(*columns)

        return QueryWithParameters(query, parameters=parameters)

    def _get_rows_query(
        self,
        *,
        table: Table | None = None,
        terms: list[Term] | None = None,
        filters: ParametersType | None = None,
    ) -> QueryWithParameters:
        table = table or Table(self.TABLE_NAME)
        terms = terms or self.columns  # type: ignore
        filters = filters or {}

        query: QueryBuilder = self._query_builder().from_(table).select(*terms)
        parameters: ParametersType = {}
        for column_name, value in filters.items():
            key: str = column_name

            query = query.where(table[column_name] == Parameter(f":{key}"))
            parameters[key] = value

        return QueryWithParameters(query, parameters=parameters)

    # crud operations

    async def _add(
        self,
        rows: list[T_ROW],
        *,
        table_name: str | None = None,
        column_names: list[str] | None = None,
    ) -> Iterable[aiosqlite.Row]:
        if column_names is None:
            column_names = self.column_names
        elif table_name is None:
            raise ValueError("Must provide table_name if column_names is provided")

        table_name = table_name or self.TABLE_NAME

        table: Table = Table(table_name)
        columns: list[Field] = [table[column] for column in column_names]

        qp: QueryWithParameters = self._create_row_query(
            rows, table=table, columns=columns
        )
        async with self.db.writer() as connection:
            _rows: Iterable[aiosqlite.Row] = await connection.execute_fetchall(
                sql=qp.query.get_sql(), parameters=qp.parameters
            )

        return _rows

    async def add(self, row: T_ROW) -> T_ROW:
        _rows: Iterable[aiosqlite.Row] = await self._add([row])
        return self._return_one(_rows, row_cls=self._row_cls).unwrap()

    async def add_many(self, rows: list[T_ROW]) -> list[T_ROW]:
        _rows: Iterable[aiosqlite.Row] = await self._add(rows)
        return self._return_many(_rows, row_cls=self._row_cls)

    async def _get(
        self,
        table_name: str | None = None,
        column_names: list[str] | None = None,
        filters: ParametersType | None = None,
    ) -> Iterable[aiosqlite.Row]:
        if column_names is None:
            column_names = self.column_names
        elif table_name is None:
            raise ValueError("Must provide table_name if column_names is provided")

        table_name = table_name or self.TABLE_NAME

        table: Table = Table(table_name)
        terms: list[Term] = [table[column_name] for column_name in column_names]

        qp: QueryWithParameters = self._get_rows_query(
            table=table, terms=terms, filters=filters
        )
        async with self.db.reader() as connection:
            rows: Iterable[aiosqlite.Row] = await connection.execute_fetchall(
                sql=qp.query.get_sql(), parameters=qp.parameters
            )

        return rows

    async def get(
        self,
        *,
        column_names: list[str] | None = None,
        filters: ParametersType | None = None,
    ) -> list[T_ROW]:
        rows: Iterable[aiosqlite.Row] = await self._get(
            column_names=column_names, filters=filters
        )
        return self._return_many(rows, row_cls=self._row_cls)

    async def get_one(
        self,
        *,
        column_names: list[str] | None = None,
        filters: ParametersType | None = None,
    ) -> Result[T_ROW, ValueError]:
        rows: Iterable[aiosqlite.Row] = await self._get(
            column_names=column_names, filters=filters
        )
        return self._return_one(rows, row_cls=self._row_cls)

    async def get_one_or_none(
        self,
        *,
        column_names: list[str] | None = None,
        filters: ParametersType | None = None,
    ) -> T_ROW | None:
        rows: Iterable[aiosqlite.Row] = await self._get(
            column_names=column_names, filters=filters
        )
        return self._return_one_or_none(rows, row_cls=self._row_cls)

    async def get_last(
        self,
        *,
        column_names: list[str] | None = None,
        filters: ParametersType | None = None,
    ) -> T_ROW | None:
        filters = filters or {}

        rows: Iterable[aiosqlite.Row] = await self._get(
            column_names=column_names,
            filters={f"rowid": "LAST_INSERT_ROWID()"} | filters,
        )
        return self._return_one_or_none(rows, row_cls=self._row_cls)
