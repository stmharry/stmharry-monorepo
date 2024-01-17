import itertools
from copy import copy
from typing import Any, Optional, Union

from pypika.enums import Dialects
from pypika.queries import AliasedQuery, Query, QueryBuilder, Selectable
from pypika.terms import (
    ArithmeticExpression,
    Criterion,
    EmptyCriterion,
    Field,
    Function,
    Star,
    Term,
    ValueWrapper,
)
from pypika.utils import QueryException, builder


class SQLQuery(Query):
    @classmethod
    def _builder(cls, **kwargs) -> "SQLQueryBuilder":
        return SQLQueryBuilder(**kwargs)


class SQLQueryBuilder(QueryBuilder):
    ALIAS_QUOTE_CHAR = '"'
    QUERY_CLS = SQLQuery

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(dialect=Dialects.SQLLITE, **kwargs)

        self._returns: list[Any] = []
        self._return_star: bool = False

        self._on_conflict: bool = False
        self._on_conflict_fields: list[Any] = []
        self._on_conflict_do_nothing: bool = False
        self._on_conflict_do_updates: list[Any] = []
        self._on_conflict_wheres: Any | None = None
        self._on_conflict_do_update_wheres: Any | None = None

        self._with_recursive: bool = False

    def __copy__(self) -> "SQLQueryBuilder":
        newone = super().__copy__()
        newone._returns = copy(self._returns)
        newone._on_conflict_do_updates = copy(self._on_conflict_do_updates)
        return newone

    @builder
    def with_aliased_query(
        self, aliased_query: AliasedQuery, recursive: bool = False
    ) -> "SQLQueryBuilder":
        self._with.append(aliased_query)
        self._with_recursive = self._with_recursive or recursive

        return self

    @builder
    def on_conflict(self, *target_fields: Union[str, Term]):
        if not self._insert_table:
            raise QueryException("On conflict only applies to insert query")

        self._on_conflict = True

        for target_field in target_fields:
            if isinstance(target_field, str):
                self._on_conflict_fields.append(self._conflict_field_str(target_field))
            elif isinstance(target_field, Term):
                self._on_conflict_fields.append(target_field)

    @builder
    def do_nothing(self):
        if len(self._on_conflict_do_updates) > 0:
            raise QueryException("Can not have two conflict handlers")
        self._on_conflict_do_nothing = True

    @builder
    def do_update(
        self, update_field: Union[str, Field], update_value: Optional[Any] = None
    ):
        if self._on_conflict_do_nothing:
            raise QueryException("Can not have two conflict handlers")

        if isinstance(update_field, str):
            field = self._conflict_field_str(update_field)
        elif isinstance(update_field, Field):
            field = update_field
        else:
            raise QueryException("Unsupported update_field")

        if update_value is not None:
            self._on_conflict_do_updates.append((field, ValueWrapper(update_value)))
        else:
            self._on_conflict_do_updates.append((field, None))

    @builder
    def where(self, criterion: Criterion):
        if not self._on_conflict:
            return super().where(criterion)

        if isinstance(criterion, EmptyCriterion):
            return

        if self._on_conflict_do_nothing:
            raise QueryException("DO NOTHING doest not support WHERE")

        if self._on_conflict_fields and self._on_conflict_do_updates:
            if self._on_conflict_do_update_wheres:
                self._on_conflict_do_update_wheres &= criterion
            else:
                self._on_conflict_do_update_wheres = criterion
        elif self._on_conflict_fields:
            if self._on_conflict_wheres:
                self._on_conflict_wheres &= criterion
            else:
                self._on_conflict_wheres = criterion
        else:
            raise QueryException("Can not have fieldless ON CONFLICT WHERE")

    @builder
    def using(self, table: Union[Selectable, str]):
        self._using.append(table)

    def _conflict_field_str(self, term: str) -> Optional[Field]:
        if self._insert_table:
            return Field(term, table=self._insert_table)

        return None

    def _on_conflict_sql(self, **kwargs: Any) -> str:
        if not self._on_conflict_do_nothing and len(self._on_conflict_do_updates) == 0:
            if not self._on_conflict_fields:
                return ""
            raise QueryException("No handler defined for on conflict")

        if self._on_conflict_do_updates and not self._on_conflict_fields:
            raise QueryException("Can not have fieldless on conflict do update")

        conflict_query = " ON CONFLICT"
        if self._on_conflict_fields:
            fields = [
                f.get_sql(with_alias=True, **kwargs) for f in self._on_conflict_fields
            ]
            conflict_query += " (" + ", ".join(fields) + ")"

        if self._on_conflict_wheres:
            conflict_query += " WHERE {where}".format(
                where=self._on_conflict_wheres.get_sql(subquery=True, **kwargs)
            )

        return conflict_query

    def _on_conflict_action_sql(self, **kwargs: Any) -> str:
        if self._on_conflict_do_nothing:
            return " DO NOTHING"
        elif len(self._on_conflict_do_updates) > 0:
            updates = []
            for field, value in self._on_conflict_do_updates:
                if value:
                    updates.append(
                        "{field}={value}".format(
                            field=field.get_sql(**kwargs),
                            value=value.get_sql(with_namespace=True, **kwargs),
                        )
                    )
                else:
                    updates.append(
                        "{field}=EXCLUDED.{value}".format(
                            field=field.get_sql(**kwargs),
                            value=field.get_sql(**kwargs),
                        )
                    )
            action_sql = " DO UPDATE SET {updates}".format(updates=",".join(updates))

            if self._on_conflict_do_update_wheres:
                action_sql += " WHERE {where}".format(
                    where=self._on_conflict_do_update_wheres.get_sql(
                        subquery=True, with_namespace=True, **kwargs
                    )
                )
            return action_sql

        return ""

    @builder
    def returning(self, *terms: Any):
        for term in terms:
            if isinstance(term, Field):
                self._return_field(term)
            elif isinstance(term, str):
                self._return_field_str(term)
            elif isinstance(term, (Function, ArithmeticExpression)):
                if term.is_aggregate:
                    raise QueryException(
                        "Aggregate functions are not allowed in returning"
                    )
                self._return_other(term)
            else:
                self._return_other(self.wrap_constant(term, self._wrapper_cls))

    def _validate_returning_term(self, term: Term) -> None:
        for field in term.fields_():
            if not any([self._insert_table, self._update_table, self._delete_from]):
                raise QueryException("Returning can't be used in this query")

            table_is_insert_or_update_table = field.table in {
                self._insert_table,
                self._update_table,
            }
            join_tables = set(
                itertools.chain.from_iterable(
                    [j.criterion.tables_ for j in self._joins]
                )
            )
            join_and_base_tables = set(self._from) | join_tables
            table_not_base_or_join = bool(term.tables_ - join_and_base_tables)
            if not table_is_insert_or_update_table and table_not_base_or_join:
                raise QueryException("You can't return from other tables")

    def _set_returns_for_star(self) -> None:
        self._returns = [
            returning for returning in self._returns if not hasattr(returning, "table")
        ]
        self._return_star = True

    def _return_field(self, term: Union[str, Field]) -> None:
        if self._return_star:
            # Do not add select terms after a star is selected
            return

        self._validate_returning_term(term)

        if isinstance(term, Star):
            self._set_returns_for_star()

        self._returns.append(term)

    def _return_field_str(self, term: Union[str, Field]) -> None:
        if term == "*":
            self._set_returns_for_star()
            self._returns.append(Star())
            return

        if self._insert_table:
            self._return_field(Field(term, table=self._insert_table))
        elif self._update_table:
            self._return_field(Field(term, table=self._update_table))
        elif self._delete_from:
            self._return_field(Field(term, table=self._from[0]))
        else:
            raise QueryException("Returning can't be used in this query")

    def _return_other(self, function: Term) -> None:
        self._validate_returning_term(function)
        self._returns.append(function)

    def _returning_sql(self, **kwargs: Any) -> str:
        return " RETURNING {returning}".format(
            returning=",".join(
                term.get_sql(with_alias=True, **kwargs) for term in self._returns
            ),
        )

    def get_sql(
        self, with_alias: bool = False, subquery: bool = False, **kwargs: Any
    ) -> str:
        self._set_kwargs_defaults(kwargs)

        querystring = super(SQLQueryBuilder, self).get_sql(
            with_alias, subquery, **kwargs
        )

        querystring += self._on_conflict_sql(**kwargs)
        querystring += self._on_conflict_action_sql(**kwargs)

        if self._returns:
            kwargs["with_namespace"] = self._update_table and self.from_
            querystring += self._returning_sql(**kwargs)
        return querystring

    def _with_sql(self, **kwargs: Any) -> str:
        return ("WITH RECURSIVE " if self._with_recursive else "WITH ") + ",".join(
            clause.name
            + " AS ("
            + clause.get_sql(subquery=False, with_alias=False, **kwargs)
            + ") "
            for clause in self._with
        )
