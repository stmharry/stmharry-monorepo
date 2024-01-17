import uuid
from collections.abc import Callable
from typing import Any, Type, TypeVar

import aiosqlite
from pydantic import BaseModel, ValidationError

from stmharry.rust.result import returns_result

T = TypeVar("T", bound="BaseRow")


class BaseRow(BaseModel):
    @classmethod
    @returns_result(err=ValidationError)
    def model_validate_row(cls: Type[T], row: aiosqlite.Row) -> T:
        keys: set[str] = set(row.keys()) & set(cls.model_fields.keys())
        obj: dict[str, Any] = {field_name: row[field_name] for field_name in keys}

        return cls.model_validate(obj)

    class Config:
        json_encoders: dict[type, Callable[[Any], Any]] = {
            uuid.UUID: str,
        }
