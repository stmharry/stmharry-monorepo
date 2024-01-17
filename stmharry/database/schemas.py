import uuid
from collections.abc import Callable
from typing import Any, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound="BaseRow")


class BaseRow(BaseModel):
    class Config:
        json_encoders: dict[type, Callable[[Any], Any]] = {
            uuid.UUID: str,
        }
